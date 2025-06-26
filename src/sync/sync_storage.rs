use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{Arc, RwLock, Mutex};

use anyhow::Result;
use s3::{Bucket, Region, creds::Credentials};

pub trait SyncTarget {
    fn list(&self, prefix: &str) -> Result<Vec<String>>;
    fn get(&self, path: &str) -> Result<Vec<u8>>;
    fn put(&self, path: &str, content: &[u8]) -> Result<()>;
}

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

impl SyncTarget for S3Storage {
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

impl SyncTarget for LocalStorage {
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

impl SyncTarget for InMemoryStorage {
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

// SyncTarget trait wrapper to allow Arc<dyn SyncTarget> to implement SyncTarget
#[derive(Clone)]
pub struct ArcStorage {
    inner: Arc<dyn SyncTarget>,
}

impl ArcStorage {
    pub fn new(target: Arc<dyn SyncTarget>) -> Self {
        Self { inner: target }
    }
}

impl SyncTarget for ArcStorage {
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
    pub fn new(inner: Box<dyn SyncTarget>, passphrase: String) -> Self {
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
        // Ensure keys are created
        self.get_or_create_keys()?;
        
        // Use age for proper encryption
        let keys_guard = self.cached_keys.lock()
            .map_err(|_| anyhow::anyhow!("Failed to acquire keys lock"))?;
        let (recipient, _) = keys_guard.as_ref().unwrap();
        let encrypted = age::encrypt(recipient, data)?;
        Ok(encrypted)
    }
    
    fn decrypt_bytes(&self, encrypted: &[u8]) -> Result<Vec<u8>> {
        // Ensure keys are created
        self.get_or_create_keys()?;
        
        // Use age for proper decryption
        let keys_guard = self.cached_keys.lock()
            .map_err(|_| anyhow::anyhow!("Failed to acquire keys lock"))?;
        let (_, identity) = keys_guard.as_ref().unwrap();
        let decrypted = age::decrypt(identity, encrypted)?;
        Ok(decrypted)
    }
    
}

impl SyncTarget for EncryptedStorage {
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        log::debug!("ENCRYPTED STORAGE LIST: prefix='{}'", prefix);
        
        // Pass through to underlying storage - paths are not encrypted
        self.inner.list(prefix)
    }
    
    fn get(&self, path: &str) -> Result<Vec<u8>> {
        log::debug!("ENCRYPTED STORAGE GET: path='{}'", path);
        let encrypted_content = self.inner.get(path)?;
        
        // Decrypt the bytes directly
        let decrypted = self.decrypt_bytes(&encrypted_content)?;
        
        log::debug!("ENCRYPTED STORAGE GET RESULT: {} bytes", decrypted.len());
        Ok(decrypted)
    }
    
    fn put(&self, path: &str, content: &[u8]) -> Result<()> {
        log::debug!("ENCRYPTED STORAGE PUT: path='{}', size={} bytes", path, content.len());
        let encrypted_content = self.encrypt_bytes(content)?;
        
        // Store encrypted bytes directly
        self.inner.put(path, &encrypted_content)?;
        
        log::debug!("ENCRYPTED STORAGE PUT RESULT: success");
        Ok(())
    }
}