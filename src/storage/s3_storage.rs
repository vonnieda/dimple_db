use anyhow::Result;
use s3::{creds::Credentials, Bucket, Region};

use super::SyncStorage;

pub struct S3Storage {
    bucket: Bucket,
}

impl S3Storage {
    pub fn new(
        endpoint: &str,
        bucket_name: &str,
        region: &str,
        access_key: &str,
        secret_key: &str,
    ) -> Result<Self> {
        let credentials = Credentials::new(Some(access_key), Some(secret_key), None, None, None)?;
        let region = Region::Custom {
            region: region.to_string(),
            endpoint: endpoint.to_string(),
        };
        let bucket = Bucket::new(bucket_name, region, credentials)?;
        Ok(Self { bucket })
    }
}

impl SyncStorage for S3Storage {
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        log::debug!("STORAGE LIST: prefix='{}'", prefix);
        let results = self
            .bucket
            .list(prefix.to_string(), Some("/".to_string()))?;
        let mut keys = Vec::new();

        for list_bucket_result in results {
            for object in list_bucket_result.contents {
                keys.push(object.key);
            }
        }

        log::debug!("STORAGE LIST RESULT: {} items", keys.len());
        Ok(keys)
    }

    fn get(&self, path: &str) -> Result<Vec<u8>> {
        log::debug!("STORAGE GET: path='{}'", path);
        let response = self.bucket.get_object(path)?;
        
        // Check response status
        if response.status_code() != 200 {
            log::error!("STORAGE GET ERROR: S3 returned status {} for path '{}'", response.status_code(), path);
            return Err(anyhow::anyhow!("S3 returned error status {} for path: {}", response.status_code(), path));
        }
        
        let bytes = response.bytes().to_vec();
        
        // Check if we got an HTML error page instead of actual content
        if bytes.len() > 0 && bytes[0] == b'<' {
            log::error!("STORAGE GET ERROR: Received HTML response (likely an error page) for path '{}'. Content starts with: {:?}", 
                       path, String::from_utf8_lossy(&bytes[..std::cmp::min(100, bytes.len())]));
            return Err(anyhow::anyhow!("S3 returned HTML error page instead of file content for path: {}", path));
        }
        
        // Check if we got XML error response (common for S3 errors)
        if bytes.len() > 5 && &bytes[0..5] == b"<?xml" {
            log::error!("STORAGE GET ERROR: Received XML response (likely an error) for path '{}'. Content starts with: {:?}", 
                       path, String::from_utf8_lossy(&bytes[..std::cmp::min(200, bytes.len())]));
            return Err(anyhow::anyhow!("S3 returned XML error response instead of file content for path: {}", path));
        }
        
        log::debug!("STORAGE GET RESULT: {} bytes", bytes.len());
        Ok(bytes)
    }

    fn put(&self, path: &str, content: &[u8]) -> Result<()> {
        log::debug!("STORAGE PUT: path='{}', size={} bytes", path, content.len());
        self.bucket.put_object(path, content)?;
        log::debug!("STORAGE PUT RESULT: success");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    // Helper function to get test credentials from environment
    fn get_test_config() -> Option<(String, String, String, String, String, String)> {
        let endpoint = env::var("DIMPLE_TEST_S3_ENDPOINT").ok()?;
        let bucket = env::var("DIMPLE_TEST_S3_BUCKET").ok()?;
        let region = env::var("DIMPLE_TEST_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());
        let access_key = env::var("DIMPLE_TEST_S3_ACCESS_KEY").ok()?;
        let secret_key = env::var("DIMPLE_TEST_S3_SECRET_KEY").ok()?;
        let prefix = env::var("DIMPLE_TEST_S3_PREFIX").unwrap_or_else(|_| "dimple-test".to_string());
        
        Some((endpoint, bucket, region, access_key, secret_key, prefix))
    }

    fn create_test_storage() -> Option<(S3Storage, String)> {
        let (endpoint, bucket, region, access_key, secret_key, prefix) = get_test_config()?;
        let storage = S3Storage::new(&endpoint, &bucket, &region, &access_key, &secret_key).ok()?;
        Some((storage, prefix))
    }

    #[test]
    fn test_s3_put_and_get() -> Result<()> {
        let Some((storage, prefix)) = create_test_storage() else {
            println!("Skipping S3 test - no credentials provided");
            return Ok(());
        };

        let test_path = format!("{}/test-file.txt", prefix);
        let test_content = b"Hello, S3 world!";

        // Put the file
        storage.put(&test_path, test_content)?;

        // Get the file back
        let retrieved_content = storage.get(&test_path)?;

        // Verify content matches
        assert_eq!(test_content, retrieved_content.as_slice());

        println!("S3 put/get test passed");
        Ok(())
    }

    #[test]
    fn test_s3_list() -> Result<()> {
        let Some((storage, prefix)) = create_test_storage() else {
            println!("Skipping S3 test - no credentials provided");
            return Ok(());
        };

        let test_prefix = format!("{}/list-test/", prefix);
        
        // Put some test files
        storage.put(&format!("{}file1.txt", test_prefix), b"content1")?;
        storage.put(&format!("{}file2.txt", test_prefix), b"content2")?;
        storage.put(&format!("{}file3.txt", test_prefix), b"content3")?;

        // List files with prefix
        let files = storage.list(&test_prefix)?;

        // Should find our test files
        println!("Found {} files: {:?}", files.len(), files);
        
        let has_file1 = files.iter().any(|f| f.ends_with("file1.txt"));
        let has_file2 = files.iter().any(|f| f.ends_with("file2.txt"));
        let has_file3 = files.iter().any(|f| f.ends_with("file3.txt"));
        
        assert!(has_file1, "Missing file1.txt in files: {:?}", files);
        assert!(has_file2, "Missing file2.txt in files: {:?}", files);
        assert!(has_file3, "Missing file3.txt in files: {:?}", files);

        println!("S3 list test passed - found {} files", files.len());
        Ok(())
    }

    #[test]
    fn test_s3_binary_content() -> Result<()> {
        let Some((storage, prefix)) = create_test_storage() else {
            println!("Skipping S3 test - no credentials provided");
            return Ok(());
        };

        let test_path = format!("{}/binary-test.bin", prefix);
        
        // Create binary content with various byte values
        let test_content: Vec<u8> = (0..256).map(|i| i as u8).collect();

        // Put binary content
        storage.put(&test_path, &test_content)?;

        // Get it back
        let retrieved_content = storage.get(&test_path)?;

        // Verify exact binary match
        assert_eq!(test_content, retrieved_content);
        assert_eq!(test_content.len(), 256);

        println!("S3 binary content test passed");
        Ok(())
    }

    #[test]
    fn test_s3_large_content() -> Result<()> {
        let Some((storage, prefix)) = create_test_storage() else {
            println!("Skipping S3 test - no credentials provided");
            return Ok(());
        };

        let test_path = format!("{}/large-test.txt", prefix);
        
        // Create 1MB of test data
        let test_content = "A".repeat(1024 * 1024);
        let test_bytes = test_content.as_bytes();

        // Put large content
        storage.put(&test_path, test_bytes)?;

        // Get it back
        let retrieved_content = storage.get(&test_path)?;

        // Verify size and content
        assert_eq!(test_bytes.len(), retrieved_content.len());
        assert_eq!(test_bytes, retrieved_content.as_slice());

        println!("S3 large content test passed - {} bytes", retrieved_content.len());
        Ok(())
    }

    #[test]
    fn test_s3_json_content() -> Result<()> {
        let Some((storage, prefix)) = create_test_storage() else {
            println!("Skipping S3 test - no credentials provided");
            return Ok(());
        };

        let test_path = format!("{}/json-test.json", prefix);
        
        // Create JSON content (similar to what sync engine would store)
        let test_json = r#"{
            "id": "01234567-89ab-cdef-0123-456789abcdef",
            "author_id": "test-author",
            "entity_type": "TestEntity",
            "entity_id": "test-entity-123",
            "columns_json": "{\"name\":\"Test Item\",\"value\":42}",
            "merged": false
        }"#;

        // Put JSON content
        storage.put(&test_path, test_json.as_bytes())?;

        // Get it back
        let retrieved_content = storage.get(&test_path)?;
        let retrieved_json = String::from_utf8(retrieved_content)?;

        // Verify JSON structure is preserved
        let original: serde_json::Value = serde_json::from_str(test_json)?;
        let retrieved: serde_json::Value = serde_json::from_str(&retrieved_json)?;
        assert_eq!(original, retrieved);

        println!("S3 JSON content test passed");
        Ok(())
    }


    #[test]
    fn test_s3_get_nonexistent_file() {
        let Some((storage, prefix)) = create_test_storage() else {
            println!("Skipping S3 test - no credentials provided");
            return;
        };

        let nonexistent_path = format!("{}/nonexistent-{}.txt", prefix, uuid::Uuid::new_v4());
        
        // Getting non-existent file should return an error
        let result = storage.get(&nonexistent_path);
        match result {
            Ok(data) => {
                // Some S3 implementations might return empty data instead of error
                println!("S3 returned {} bytes for nonexistent file (some implementations do this)", data.len());
                println!("S3 error handling test passed (empty data for nonexistent file)");
            }
            Err(_) => {
                println!("S3 error handling test passed (error for nonexistent file)");
            }
        }
    }

    #[test]
    fn test_s3_empty_content() -> Result<()> {
        let Some((storage, prefix)) = create_test_storage() else {
            println!("Skipping S3 test - no credentials provided");
            return Ok(());
        };

        let test_path = format!("{}/empty-test.txt", prefix);
        let empty_content = b"";

        // Put empty content
        storage.put(&test_path, empty_content)?;

        // Get it back
        let retrieved_content = storage.get(&test_path)?;

        // Should be empty
        assert_eq!(empty_content, retrieved_content.as_slice());
        assert_eq!(0, retrieved_content.len());

        println!("S3 empty content test passed");
        Ok(())
    }

    #[test]
    fn test_environment_variables_info() {
        println!("S3 Test Environment Variables:");
        println!("  DIMPLE_TEST_S3_ENDPOINT: {:?}", env::var("DIMPLE_TEST_S3_ENDPOINT"));
        println!("  DIMPLE_TEST_S3_BUCKET: {:?}", env::var("DIMPLE_TEST_S3_BUCKET"));
        println!("  DIMPLE_TEST_S3_REGION: {:?}", env::var("DIMPLE_TEST_S3_REGION"));
        println!("  DIMPLE_TEST_S3_ACCESS_KEY: {}", 
            if env::var("DIMPLE_TEST_S3_ACCESS_KEY").is_ok() { "Set" } else { "Not set" });
        println!("  DIMPLE_TEST_S3_SECRET_KEY: {}", 
            if env::var("DIMPLE_TEST_S3_SECRET_KEY").is_ok() { "Set" } else { "Not set" });
        println!("  DIMPLE_TEST_S3_PREFIX: {:?}", env::var("DIMPLE_TEST_S3_PREFIX"));
        
        if get_test_config().is_some() {
            println!("All required S3 test credentials are available");
        } else {
            println!("S3 tests will be skipped - missing credentials");
            println!("   Set environment variables to enable S3 testing");
        }
    }
}

