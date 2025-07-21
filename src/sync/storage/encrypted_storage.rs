use std::sync::{Arc};

use age::secrecy::SecretString;
use anyhow::Result;

use super::{ArcStorage, SyncStorage};

/// EncryptedStorage transparently encrypts another Storage using age with
/// passphrase-derived keys
pub struct EncryptedStorage {
    inner: ArcStorage,
    recipient: age::scrypt::Recipient,
    identity: age::scrypt::Identity,
}

impl EncryptedStorage {
    pub fn new(inner: Box<dyn SyncStorage>, passphrase: String) -> Self {
        let secret = SecretString::from(passphrase.clone());
        let recipient = age::scrypt::Recipient::new(secret.clone());
        let identity = age::scrypt::Identity::new(secret);
        Self { 
            inner: ArcStorage::new(Arc::from(inner)), 
            recipient,
            identity,
        }
    }
    
    fn encrypt_bytes(&self, data: &[u8]) -> Result<Vec<u8>> {
        let encrypted = age::encrypt(&self.recipient, data)?;
        Ok(encrypted)
    }
    
    fn decrypt_bytes(&self, encrypted: &[u8]) -> Result<Vec<u8>> {
        let decrypted = age::decrypt(&self.identity, encrypted)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::storage::InMemoryStorage;

    #[test]
    fn encrypt_decrypt_roundtrip() -> Result<()> {
        let inner = Box::new(InMemoryStorage::new());
        let storage = EncryptedStorage::new(inner, "test passphrase".to_string());
        
        let test_data = b"Hello, encrypted world!";
        let path = "test/file.txt";
        
        // Store encrypted data
        storage.put(path, test_data)?;
        
        // Retrieve and decrypt
        let retrieved = storage.get(path)?;
        
        assert_eq!(test_data, retrieved.as_slice());

        // TODO assert that the test_data is not visible in the inner storage
        Ok(())
    }

    #[test]
    #[ignore]
    fn encryption_actually_encrypts() -> Result<()> {
        let inner = InMemoryStorage::new();
        let storage = EncryptedStorage::new(Box::new(inner), "test passphrase".to_string());
        
        let test_data = b"Secret message that should be encrypted";
        let path = "secret/message.txt";
        
        storage.put(path, test_data)?;
        
        // Create a second storage to verify the data is actually encrypted
        // by trying to decrypt with a different passphrase
        let wrong_passphrase_storage = EncryptedStorage::new(
            Box::new(InMemoryStorage::new()), 
            "wrong passphrase".to_string()
        );
        
        // Get the encrypted data from the first storage's inner storage
        let encrypted_data = storage.inner.get(path)?;
        
        // Put it in the second storage and try to decrypt
        wrong_passphrase_storage.inner.put(path, &encrypted_data)?;
        let decrypt_result = wrong_passphrase_storage.get(path);
        
        // Should fail because passphrase is wrong
        assert!(decrypt_result.is_err());
        
        Ok(())
    }

    #[test]
    #[ignore]
    fn list_passes_through() -> Result<()> {
        let inner = Box::new(InMemoryStorage::new());
        let storage = EncryptedStorage::new(inner, "test passphrase".to_string());
        
        // Put some files
        storage.put("test/file1.txt", b"data1")?;
        storage.put("test/file2.txt", b"data2")?;
        storage.put("other/file3.txt", b"data3")?;
        
        // List should work normally (paths are not encrypted)
        let files = storage.list("test/")?;
        assert_eq!(files.len(), 2);
        assert!(files.contains(&"test/file1.txt".to_string()));
        assert!(files.contains(&"test/file2.txt".to_string()));
        
        Ok(())
    }

    #[test]
    #[ignore]
    fn different_passphrases_incompatible() -> Result<()> {
        let inner1 = Box::new(InMemoryStorage::new());
        let inner2 = Box::new(InMemoryStorage::new());
        
        let storage1 = EncryptedStorage::new(inner1, "passphrase1".to_string());
        let storage2 = EncryptedStorage::new(inner2, "passphrase2".to_string());
        
        let test_data = b"Secret data";
        let path = "test/secret.txt";
        
        // Store with first passphrase
        storage1.put(path, test_data)?;
        let encrypted_data = storage1.inner.get(path)?;
        
        // Try to read with second passphrase by putting the encrypted data in storage2
        storage2.inner.put(path, &encrypted_data)?;
        
        // This should fail because the passphrases are different
        let result = storage2.get(path);
        assert!(result.is_err());
        
        Ok(())
    }
}
