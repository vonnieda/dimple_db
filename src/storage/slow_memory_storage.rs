use std::{collections::HashMap, sync::{Arc, RwLock}, time::Duration};

use anyhow::Result;

use super::SyncStorage;

/// In-memory storage with artificial delays to simulate high-latency storage like S3
pub struct SlowInMemoryStorage {
    data: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    get_delay_ms: u64,
    put_delay_ms: u64,
    list_delay_ms: u64,
}

impl SlowInMemoryStorage {
    pub fn new(get_delay_ms: u64, put_delay_ms: u64, list_delay_ms: u64) -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            get_delay_ms,
            put_delay_ms,
            list_delay_ms,
        }
    }

    /// Create a storage that simulates S3-like latency
    pub fn s3_like() -> Self {
        Self::new(25, 250, 50) // GET: 25ms, PUT: 250ms, LIST: 50ms
    }
}

impl Default for SlowInMemoryStorage {
    fn default() -> Self {
        Self::s3_like()
    }
}

impl SyncStorage for SlowInMemoryStorage {
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        log::debug!("SLOW STORAGE LIST: prefix='{}' (delay: {}ms)", prefix, self.list_delay_ms);
        std::thread::sleep(Duration::from_millis(self.list_delay_ms));
        
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
        log::debug!("SLOW STORAGE LIST RESULT: {} items", results.len());
        Ok(results)
    }

    fn get(&self, path: &str) -> Result<Vec<u8>> {
        log::debug!("SLOW STORAGE GET: path='{}' (delay: {}ms)", path, self.get_delay_ms);
        std::thread::sleep(Duration::from_millis(self.get_delay_ms));
        
        let data = self
            .data
            .read()
            .map_err(|_| anyhow::anyhow!("Failed to acquire read lock"))?;
        let content = data
            .get(path)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Path not found: {}", path))?;
        log::debug!("SLOW STORAGE GET RESULT: {} bytes", content.len());
        Ok(content)
    }

    fn put(&self, path: &str, content: &[u8]) -> Result<()> {
        log::debug!("SLOW STORAGE PUT: path='{}', size={} bytes (delay: {}ms)", path, content.len(), self.put_delay_ms);
        std::thread::sleep(Duration::from_millis(self.put_delay_ms));
        
        let mut data = self
            .data
            .write()
            .map_err(|_| anyhow::anyhow!("Failed to acquire write lock"))?;
        data.insert(path.to_string(), content.to_vec());
        log::debug!("SLOW STORAGE PUT RESULT: success");
        Ok(())
    }
}

impl Clone for SlowInMemoryStorage {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            get_delay_ms: self.get_delay_ms,
            put_delay_ms: self.put_delay_ms,
            list_delay_ms: self.list_delay_ms,
        }
    }
}