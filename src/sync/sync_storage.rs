use std::sync::{Arc};

use anyhow::Result;

pub trait SyncStorage {
    fn list(&self, prefix: &str) -> Result<Vec<String>>;
    fn get(&self, path: &str) -> Result<Vec<u8>>;
    fn put(&self, path: &str, content: &[u8]) -> Result<()>;
}

// SyncStorage trait wrapper to allow Arc<dyn SyncStorage> to implement SyncStorage
#[derive(Clone)]
pub struct ArcStorage {
    inner: Arc<dyn SyncStorage>,
}

impl ArcStorage {
    pub fn new(target: Arc<dyn SyncStorage>) -> Self {
        Self { inner: target }
    }
}

impl SyncStorage for ArcStorage {
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }

    fn get(&self, path: &str) -> Result<Vec<u8>> {
        self.inner.get(path)
    }

    fn put(&self, path: &str, content: &[u8]) -> Result<()> {
        self.inner.put(path, content)
    }
}

