use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::sync::{ArcStorage, SyncStorage};

/// EncryptedStorage provides transparent encryption for file content only.
/// 
/// This implementation encrypts:
/// - File content: Encrypted using age with passphrase-derived keys
/// 
/// File paths are stored in plaintext for simplicity. The security benefit
/// of encrypting paths is minimal since they only reveal that someone is
/// using Dimple Data for synchronization.
#[derive(Clone)]
pub struct EncryptedStorage {
    inner: ArcStorage,
    passphrase: String,
    // Lazily initialized and cached for performance
    cached_keys: Arc<Mutex<Option<(age::scrypt::Recipient, age::scrypt::Identity)>>>,
}

impl EncryptedStorage {
    pub fn new(inner: Box<dyn SyncStorage>, passphrase: String) -> Self {
        Self { 
            inner: ArcStorage::new(Arc::from(inner)), 
            passphrase,
            cached_keys: Arc::new(Mutex::new(None)),
        }
    }
    
    fn get_or_create_keys(&self) -> Result<()> {
        let mut keys_guard = self.cached_keys.lock()
            .map_err(|_| anyhow::anyhow!("Failed to acquire keys lock"))?;
        
        if keys_guard.is_none() {
            use age::secrecy::SecretString;
            let secret = SecretString::from(self.passphrase.clone());
            let recipient = age::scrypt::Recipient::new(secret.clone());
            let identity = age::scrypt::Identity::new(secret);
            *keys_guard = Some((recipient, identity));
        }
        
        Ok(())
    }

    
    fn encrypt_bytes(&self, data: &[u8]) -> Result<Vec<u8>> {
        self.get_or_create_keys()?;        
        let keys_guard = self.cached_keys.lock()
            .map_err(|_| anyhow::anyhow!("Failed to acquire keys lock"))?;
        let (recipient, _) = keys_guard.as_ref().unwrap();
        let encrypted = age::encrypt(recipient, data)?;
        Ok(encrypted)
    }
    
    fn decrypt_bytes(&self, encrypted: &[u8]) -> Result<Vec<u8>> {
        self.get_or_create_keys()?;
        let keys_guard = self.cached_keys.lock()
            .map_err(|_| anyhow::anyhow!("Failed to acquire keys lock"))?;
        let (_, identity) = keys_guard.as_ref().unwrap();
        let decrypted = age::decrypt(identity, encrypted)?;
        Ok(decrypted)
    }
    
}

impl SyncStorage for EncryptedStorage {
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        log::debug!("ENCRYPTED STORAGE LIST: prefix='{}'", prefix);        
        // Pass through to underlying storage - paths are not encrypted
        self.inner.list(prefix)
    }
    
    fn get(&self, path: &str) -> Result<Vec<u8>> {
        log::debug!("ENCRYPTED STORAGE GET: path='{}'", path);
        let encrypted_content = self.inner.get(path)?;        
        let decrypted = self.decrypt_bytes(&encrypted_content)?;
        log::debug!("ENCRYPTED STORAGE GET RESULT: {} bytes", decrypted.len());
        Ok(decrypted)
    }
    
    fn put(&self, path: &str, content: &[u8]) -> Result<()> {
        log::debug!("ENCRYPTED STORAGE PUT: path='{}', size={} bytes", path, content.len());
        let encrypted_content = self.encrypt_bytes(content)?;
        self.inner.put(path, &encrypted_content)?;
        log::debug!("ENCRYPTED STORAGE PUT RESULT: success");
        Ok(())
    }
}
