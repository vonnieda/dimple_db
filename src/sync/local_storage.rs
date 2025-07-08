use std::{fs, path::Path};

use anyhow::Result;

use crate::sync::SyncStorage;

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
        let full_path = format!("{}/{}", self.base_path, prefix);
        let path = Path::new(&full_path);

        if !path.exists() {
            log::debug!("STORAGE LIST RESULT: 0 items (path does not exist)");
            return Ok(Vec::new());
        }

        let mut results = Vec::new();
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let file_name = entry.file_name().to_string_lossy().to_string();
            results.push(format!("{}/{}", prefix, file_name));
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
