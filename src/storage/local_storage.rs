use std::{fs, path::Path};

use anyhow::Result;

use super::SyncStorage;

pub struct LocalStorage {
    base_path: String,
}

impl LocalStorage {
    pub fn new(base_path: &str) -> Self {
        Self {
            base_path: base_path.to_string(),
        }
    }
}

impl SyncStorage for LocalStorage {
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        log::debug!("STORAGE LIST: prefix='{}'", prefix);
        // Remove trailing slash from prefix to normalize it
        let normalized_prefix = prefix.trim_end_matches('/');
        let full_path = format!("{}/{}", self.base_path, normalized_prefix);
        let path = Path::new(&full_path);

        if !path.exists() {
            log::debug!("STORAGE LIST RESULT: 0 items (path does not exist)");
            return Ok(Vec::new());
        }

        let mut results = Vec::new();
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let file_name = entry.file_name().to_string_lossy().to_string();
            // Handle empty prefix case
            if normalized_prefix.is_empty() {
                results.push(format!("/{}", file_name));
            } else {
                results.push(format!("{}/{}", normalized_prefix, file_name));
            }
        }

        log::debug!("STORAGE LIST RESULT: {} items", results.len());
        Ok(results)
    }

    fn get(&self, path: &str) -> Result<Vec<u8>> {
        log::debug!("STORAGE GET: path='{}'", path);
        let full_path = format!("{}/{}", self.base_path, path);
        let content = fs::read(full_path)?;
        log::debug!("STORAGE GET RESULT: {} bytes", content.len());
        Ok(content)
    }

    fn put(&self, path: &str, content: &[u8]) -> Result<()> {
        log::debug!("STORAGE PUT: path='{}', size={} bytes", path, content.len());
        let full_path = format!("{}/{}", self.base_path, path);
        if let Some(parent) = Path::new(&full_path).parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(full_path, content)?;
        log::debug!("STORAGE PUT RESULT: success");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_list_with_empty_prefix() {
        let temp_dir = TempDir::new().unwrap();
        let storage = LocalStorage::new(temp_dir.path().to_str().unwrap());
        
        // Create some files in the root
        storage.put("file1.txt", b"content1").unwrap();
        storage.put("file2.txt", b"content2").unwrap();
        
        // List with empty prefix should return all files in root
        let files = storage.list("").unwrap();
        assert_eq!(files.len(), 2);
        assert!(files.contains(&"/file1.txt".to_string()));
        assert!(files.contains(&"/file2.txt".to_string()));
    }

    #[test]
    fn test_list_with_non_existent_prefix() {
        let temp_dir = TempDir::new().unwrap();
        let storage = LocalStorage::new(temp_dir.path().to_str().unwrap());
        
        // List a non-existent directory
        let files = storage.list("non_existent_dir").unwrap();
        assert_eq!(files.len(), 0);
    }

    #[test]
    fn test_list_with_nested_directories() {
        let temp_dir = TempDir::new().unwrap();
        let storage = LocalStorage::new(temp_dir.path().to_str().unwrap());
        
        // Create nested structure
        storage.put("dir1/file1.txt", b"content1").unwrap();
        storage.put("dir1/file2.txt", b"content2").unwrap();
        storage.put("dir1/subdir/file3.txt", b"content3").unwrap();
        storage.put("dir2/file4.txt", b"content4").unwrap();
        
        // List dir1 - should only show immediate children
        let files = storage.list("dir1").unwrap();
        assert_eq!(files.len(), 3); // file1.txt, file2.txt, and subdir
        assert!(files.contains(&"dir1/file1.txt".to_string()));
        assert!(files.contains(&"dir1/file2.txt".to_string()));
        assert!(files.contains(&"dir1/subdir".to_string()));
        
        // List dir1/subdir
        let files = storage.list("dir1/subdir").unwrap();
        assert_eq!(files.len(), 1);
        assert!(files.contains(&"dir1/subdir/file3.txt".to_string()));
    }

    #[test]
    fn test_list_with_trailing_slash() {
        let temp_dir = TempDir::new().unwrap();
        let storage = LocalStorage::new(temp_dir.path().to_str().unwrap());
        
        // Create files
        storage.put("dir/file.txt", b"content").unwrap();
        
        // Both "dir" and "dir/" should work the same after normalization
        let files1 = storage.list("dir").unwrap();
        let files2 = storage.list("dir/").unwrap();
        
        assert_eq!(files1.len(), 1);
        assert_eq!(files2.len(), 1); // Should be the same after fixing trailing slash handling
        assert_eq!(files1, files2); // Both should return identical results
    }
}

