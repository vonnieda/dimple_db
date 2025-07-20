use anyhow::Result;
use s3::{creds::Credentials, Bucket, Region};

use super::SyncStorage;

pub struct S3Storage {
    bucket: Bucket,
}

impl S3Storage {
    pub fn new(
        _endpoint: &str,
        bucket_name: &str,
        region: &str,
        access_key: &str,
        secret_key: &str,
    ) -> Result<Self> {
        let credentials = Credentials::new(Some(access_key), Some(secret_key), None, None, None)?;
        let region = region.parse::<Region>()?;
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
        let bytes = response.bytes().to_vec();
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

