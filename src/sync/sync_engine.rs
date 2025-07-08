use anyhow::Result;

use crate::{sync::{encrypted_storage::EncryptedStorage, s3_storage::S3Storage, SyncStorage}, Db};

pub struct SyncEngine {
    storage: Box<dyn SyncStorage>,
}

impl SyncEngine {
    pub fn new_with_storage(storage: Box<dyn SyncStorage>) -> Result<Self> {
        Ok(SyncEngine {
            storage,
        })
    }

    pub fn builder() -> SyncEngineBuilder {
        SyncEngineBuilder::default()
    }

    pub fn sync(&self, db: &Db) -> Result<()> {
        todo!()
    }
}

#[derive(Default)]
pub struct SyncEngineBuilder {
    storage: Option<Box<dyn SyncStorage>>,
    passphrase: Option<String>,
}

impl SyncEngineBuilder {
    pub fn in_memory(mut self) -> Self {
        self
    }

    pub fn s3(mut self, endpoint: &str,
        bucket_name: &str,
        region: &str,
        access_key: &str,
        secret_key: &str) -> Result<Self> {
        self.storage = Some(Box::new(S3Storage::new(endpoint, bucket_name, region, 
            access_key, secret_key)?));
        Ok(self)
    }

    pub fn encrypted(mut self, passphrase: &str) -> Self {
        self.passphrase = Some(passphrase.to_string());
        self
    }

    pub fn build(mut self) -> Result<SyncEngine> {
        if let Some(passphrase) = self.passphrase {
            let storage = EncryptedStorage::new(self.storage.unwrap(), passphrase);
            SyncEngine::new_with_storage(Box::new(storage))
        }
        else {
            SyncEngine::new_with_storage(self.storage.unwrap())
        }
    }
}