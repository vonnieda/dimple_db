use std::{collections::HashMap, sync::{Arc, RwLock}};

use anyhow::Result;

use crate::sync::SyncStorage;

pub struct InMemoryStorage {
    data: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl SyncStorage for InMemoryStorage {
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        log::debug!("STORAGE LIST: prefix='{}'", prefix);
        let data = self
            .data
            .read()
            .map_err(|_| anyhow::anyhow!("Failed to acquire read lock"))?;
        let mut results = Vec::new();

        for key in data.keys() {
            if key.starts_with(prefix) {
                results.push(key.clone());
            }
        }

        results.sort();
        log::debug!("STORAGE LIST RESULT: {} items", results.len());
        Ok(results)
    }

    fn get(&self, path: &str) -> Result<Vec<u8>> {
        log::debug!("STORAGE GET: path='{}'", path);
        let data = self
            .data
            .read()
            .map_err(|_| anyhow::anyhow!("Failed to acquire read lock"))?;
        let content = data
            .get(path)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Path not found: {}", path))?;
        log::debug!("STORAGE GET RESULT: {} bytes", content.len());
        Ok(content)
    }

    fn put(&self, path: &str, content: &[u8]) -> Result<()> {
        log::debug!("STORAGE PUT: path='{}', size={} bytes", path, content.len());
        let mut data = self
            .data
            .write()
            .map_err(|_| anyhow::anyhow!("Failed to acquire write lock"))?;
        data.insert(path.to_string(), content.to_vec());
        log::debug!("STORAGE PUT RESULT: success");
        Ok(())
    }
}

impl Clone for InMemoryStorage {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
        }
    }
}
