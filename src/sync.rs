use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::Result;
use s3::{Bucket, Region, creds::Credentials};
use serde::{Deserialize, Serialize};

use crate::db::{Change, Db};

pub trait Storage {
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

impl Storage for S3Storage {
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

impl Storage for LocalStorage {
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

impl Storage for InMemoryStorage {
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

// Storage trait wrapper to allow Arc<dyn Storage> to implement Storage
#[derive(Clone)]
pub struct ArcStorage {
    inner: Arc<dyn Storage>,
}

impl ArcStorage {
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self { inner: storage }
    }
}

impl Storage for ArcStorage {
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
}

impl EncryptedStorage {
    pub fn new(inner: Box<dyn Storage>, passphrase: String) -> Self {
        Self { 
            inner: ArcStorage::new(Arc::from(inner)), 
            passphrase,
        }
    }
    
    fn create_recipient(&self) -> Result<age::scrypt::Recipient> {
        use age::secrecy::SecretString;
        let secret = SecretString::from(self.passphrase.clone());
        Ok(age::scrypt::Recipient::new(secret))
    }
    
    fn create_identity(&self) -> Result<age::scrypt::Identity> {
        use age::secrecy::SecretString;
        let secret = SecretString::from(self.passphrase.clone());
        Ok(age::scrypt::Identity::new(secret))
    }

    
    fn encrypt_bytes(&self, data: &[u8]) -> Result<Vec<u8>> {
        // Use age for proper encryption
        let recipient = self.create_recipient()?;
        let encrypted = age::encrypt(&recipient, data)?;
        Ok(encrypted)
    }
    
    fn decrypt_bytes(&self, encrypted: &[u8]) -> Result<Vec<u8>> {
        // Use age for proper decryption
        let identity = self.create_identity()?;
        let decrypted = age::decrypt(&identity, encrypted)?;
        Ok(decrypted)
    }
    
}

impl Storage for EncryptedStorage {
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

#[derive(Debug, Clone)]
pub struct SyncConfig {
    pub endpoint: String,
    pub bucket: String,
    pub base_path: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    pub change_batch_window: Duration,
    pub sync_interval: Duration,
    pub passphrase: Option<String>,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            endpoint: "".to_string(),
            bucket: "".to_string(),
            base_path: "".to_string(),
            access_key: "".to_string(),
            secret_key: "".to_string(),
            region: "us-east-1".to_string(),
            change_batch_window: Duration::from_secs(24 * 60 * 60), // 24 hours
            sync_interval: Duration::from_secs(60),                 // 1 minute
            passphrase: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChangeBundle {
    pub author: String,
    pub changes: Vec<Change>,
    pub timestamp_range: TimestampRange,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TimestampRange {
    pub start: i64,
    pub end: i64,
}

pub struct SyncClient {
    pub config: SyncConfig,
    pub storage: Box<dyn Storage>,
}

impl SyncClient {
    pub fn new_with_s3(config: SyncConfig) -> Result<Self> {
        let storage = S3Storage::new(
            &config.endpoint,
            &config.bucket,
            &config.region,
            &config.access_key,
            &config.secret_key,
        )?;

        let storage: Box<dyn Storage> = if let Some(passphrase) = &config.passphrase {
            Box::new(EncryptedStorage::new(Box::new(storage), passphrase.clone()))
        } else {
            Box::new(storage)
        };

        Ok(Self { config, storage })
    }

    pub fn new_with_local(config: SyncConfig, base_path: &str) -> Self {
        let storage = LocalStorage::new(base_path);
        
        let storage: Box<dyn Storage> = if let Some(passphrase) = &config.passphrase {
            Box::new(EncryptedStorage::new(Box::new(storage), passphrase.clone()))
        } else {
            Box::new(storage)
        };
        
        Self { config, storage }
    }

    pub fn new_with_memory(config: SyncConfig) -> Self {
        let storage = InMemoryStorage::new();
        
        let storage: Box<dyn Storage> = if let Some(passphrase) = &config.passphrase {
            Box::new(EncryptedStorage::new(Box::new(storage), passphrase.clone()))
        } else {
            Box::new(storage)
        };
        
        Self { config, storage }
    }

    pub fn sync(&self, db: &Db) -> Result<()> {
        log::info!("Starting sync for author {}", db.get_author());

        // 1. Pull changes from other authors
        let remote_changes = self.pull_remote_changes(db.get_author(), db)?;

        // 2. Apply remote changes to local database
        self.apply_remote_changes(db, &remote_changes)?;

        // 3. Get new local changes since last push and push them
        self.push_new_changes(db)?;

        log::info!("Sync completed successfully");
        Ok(())
    }

    fn pull_remote_changes(&self, our_author: &str, db: &Db) -> Result<Vec<ChangeBundle>> {
        log::info!("Pulling incremental remote changes");

        // List all authors (directories) in the bucket
        let authors_prefix = if self.config.base_path.is_empty() {
            "authors/".to_string()
        } else {
            format!("{}/authors/", self.config.base_path)
        };
        let author_dirs = self.storage.list(&authors_prefix)?;

        let mut all_changes = Vec::new();

        for author_dir in author_dirs {
            // Skip our own author
            if author_dir.contains(our_author) {
                continue;
            }

            // Extract author name from path
            let author_name =
                if let Some(author) = author_dir.trim_end_matches('/').split('/').last() {
                    author
                } else {
                    continue;
                };

            // Get the latest UUID we have locally for this author
            let latest_local_uuid = db.get_latest_uuid_for_author(author_name)?;

            log::info!(
                "Latest local UUID for author {}: {}",
                author_name,
                latest_local_uuid
            );

            // Get only changes since our latest local UUID
            let author_changes =
                self.get_author_changes_since_uuid(&author_dir, &latest_local_uuid)?;

            if !author_changes.is_empty() {
                log::info!(
                    "Found {} new change bundles from author {}",
                    author_changes.len(),
                    author_name
                );
                all_changes.extend(author_changes);
            }
        }

        log::info!(
            "Pulled {} new change bundles from remote authors",
            all_changes.len()
        );
        Ok(all_changes)
    }

    fn get_author_changes_since_uuid(
        &self,
        author_path: &str,
        since_uuid: &str,
    ) -> Result<Vec<ChangeBundle>> {
        // List change files for this author
        let mut change_files = self.storage.list(author_path)?;

        // Sort files by name (which contain UUIDs, so they'll be in chronological order)
        change_files.sort();

        let mut bundles = Vec::new();

        // If since_uuid is the null UUID, return all files
        let is_null_uuid = since_uuid == "00000000-0000-0000-0000-000000000000";

        for file_path in change_files {
            if !file_path.ends_with(".json") {
                continue;
            }

            // Extract UUID from filename
            if let Some(filename) = file_path.split('/').last() {
                if let Some(uuid_part) = filename
                    .strip_prefix("changes-")
                    .and_then(|s| s.strip_suffix(".json"))
                {
                    // If null UUID, include all files; otherwise only include files with UUID > since_uuid
                    if !is_null_uuid && uuid_part <= since_uuid {
                        continue; // Skip files up to and including since_uuid
                    }
                }
            }

            // Download and parse the change bundle
            let content_bytes = self.storage.get(&file_path)?;
            let content = String::from_utf8(content_bytes)
                .map_err(|e| anyhow::anyhow!("Failed to decode UTF-8: {}", e))?;
            let bundle: ChangeBundle = serde_json::from_str(&content)?;
            bundles.push(bundle);
        }

        Ok(bundles)
    }

    fn apply_remote_changes(&self, db: &Db, change_bundles: &[ChangeBundle]) -> Result<()> {
        log::info!("Applying {} change bundles", change_bundles.len());

        // Apply each bundle separately to preserve author information
        for bundle in change_bundles {
            if !bundle.changes.is_empty() {
                db.apply_remote_changes_with_author(&bundle.changes, &bundle.author)?;
                log::info!(
                    "Successfully applied {} changes from author {}",
                    bundle.changes.len(),
                    bundle.author
                );
            }
        }

        Ok(())
    }

    fn push_local_changes(&self, changes: &[Change], author: &str) -> Result<()> {
        if changes.is_empty() {
            return Ok(());
        }

        log::info!("Pushing {} local changes", changes.len());

        // Create change bundle
        let start_timestamp = if let Some(first_change) = changes.first() {
            self.extract_timestamp_from_change(first_change)?
        } else {
            0
        };
        let end_timestamp = if let Some(last_change) = changes.last() {
            self.extract_timestamp_from_change(last_change)?
        } else {
            0
        };

        let timestamp_range = TimestampRange {
            start: start_timestamp,
            end: end_timestamp,
        };

        let bundle = ChangeBundle {
            author: author.to_string(),
            changes: changes.to_vec(),
            timestamp_range,
        };

        // Generate filename based on latest change UUID
        let latest_change_id = if let Some(last_change) = changes.last() {
            &last_change.id
        } else {
            return Ok(()); // No changes to push
        };

        let filename = format!("changes-{}.json", latest_change_id);

        let path = if self.config.base_path.is_empty() {
            format!("authors/{}/{}", author, filename)
        } else {
            format!("{}/authors/{}/{}", self.config.base_path, author, filename)
        };

        // Upload to storage
        let content = serde_json::to_string(&bundle)?;
        self.storage.put(&path, content.as_bytes())?;

        log::info!("Pushed changes to {}", path);
        Ok(())
    }

    fn extract_timestamp_from_change(&self, change: &Change) -> Result<i64> {
        // Extract timestamp from UUIDv7 change ID
        // UUIDv7 encodes timestamp in the first 48 bits
        if let Ok(uuid) = uuid::Uuid::parse_str(&change.id) {
            if uuid.get_version() == Some(uuid::Version::SortRand) {
                // UUIDv7 version
                // Extract timestamp from UUIDv7 (first 48 bits are milliseconds since Unix epoch)
                let bytes = uuid.as_bytes();
                let timestamp_ms = u64::from_be_bytes([
                    0, 0, // pad to 8 bytes
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5],
                ]);
                return Ok(timestamp_ms as i64);
            }
        }

        // If we can't extract timestamp from UUIDv7, this is an error
        Err(anyhow::anyhow!(
            "Failed to extract timestamp from change ID: {}",
            change.id
        ))
    }

    fn get_latest_pushed_uuid(&self, author: &str) -> Result<String> {
        // List files for this author to find the latest pushed UUID
        let author_prefix = if self.config.base_path.is_empty() {
            format!("authors/{}/", author)
        } else {
            format!("{}/authors/{}/", self.config.base_path, author)
        };

        let mut files = self.storage.list(&author_prefix)?;

        // Sort filenames - since they contain UUIDs, the latest will be last
        files.sort();

        // Extract UUID from the latest filename
        if let Some(latest_file) = files.last() {
            if let Some(filename) = latest_file.split('/').last() {
                if let Some(uuid_part) = filename
                    .strip_prefix("changes-")
                    .and_then(|s| s.strip_suffix(".json"))
                {
                    return Ok(uuid_part.to_string());
                }
            }
        }

        // Return null UUID if no files found
        Ok("00000000-0000-0000-0000-000000000000".to_string())
    }

    fn push_new_changes(&self, db: &Db) -> Result<()> {
        let author = db.get_author();

        // Get the UUID of the latest pushed change for this author
        let latest_pushed_uuid = self.get_latest_pushed_uuid(author)?;

        log::info!(
            "Latest pushed UUID for author {}: {}",
            author,
            latest_pushed_uuid
        );

        // Get changes since the latest pushed UUID
        let new_changes = db.get_changes_since_uuid(&latest_pushed_uuid)?;

        if new_changes.is_empty() {
            log::info!("No new changes to push for author {}", author);
            return Ok(());
        }

        log::info!(
            "Found {} new changes to push for author {}",
            new_changes.len(),
            author
        );

        // Push only the new changes
        self.push_local_changes(&new_changes, author)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_config_default() {
        let config = SyncConfig::default();
        assert_eq!(config.region, "us-east-1");
        assert_eq!(
            config.change_batch_window,
            Duration::from_secs(24 * 60 * 60)
        );
        assert_eq!(config.sync_interval, Duration::from_secs(60));
    }

    #[test]
    fn test_change_bundle_serialization() -> Result<()> {
        let bundle = ChangeBundle {
            author: "test-author".to_string(),
            changes: vec![],
            timestamp_range: TimestampRange {
                start: 100,
                end: 200,
            },
        };

        let json = serde_json::to_string(&bundle)?;
        let deserialized: ChangeBundle = serde_json::from_str(&json)?;

        assert_eq!(bundle.author, deserialized.author);
        assert_eq!(
            bundle.timestamp_range.start,
            deserialized.timestamp_range.start
        );
        assert_eq!(bundle.timestamp_range.end, deserialized.timestamp_range.end);

        Ok(())
    }

    #[test]
    fn test_in_memory_storage() -> Result<()> {
        let storage = InMemoryStorage::new();

        // Test put and get
        storage.put("test/path/file.json", b"{\"test\": \"data\"}")?;
        let content = storage.get("test/path/file.json")?;
        assert_eq!(content, b"{\"test\": \"data\"}");

        // Test list with prefix
        storage.put("test/path/file1.json", b"data1")?;
        storage.put("test/path/file2.json", b"data2")?;
        storage.put("other/file.json", b"other")?;

        let files = storage.list("test/path/")?;
        assert_eq!(files.len(), 3); // file.json, file1.json, file2.json
        assert!(files.contains(&"test/path/file.json".to_string()));
        assert!(files.contains(&"test/path/file1.json".to_string()));
        assert!(files.contains(&"test/path/file2.json".to_string()));
        assert!(!files.contains(&"other/file.json".to_string()));

        // Test get non-existent file
        let result = storage.get("non/existent.json");
        assert!(result.is_err());

        Ok(())
    }

    #[derive(serde::Serialize, serde::Deserialize, Clone, Default, Debug)]
    struct TestEntity {
        key: String,
        name: String,
        value: i32,
    }

    #[test]
    fn test_sync_single_database() -> Result<()> {
        // Initialize debug logging for tests
        let _ = env_logger::Builder::from_default_env()
            .filter_level(log::LevelFilter::Debug)
            .is_test(true)
            .try_init();

        // Create a database and set up table
        let db = crate::db::Db::open_memory()?;
        db.migrate(&["CREATE TABLE TestEntity (
                key TEXT NOT NULL PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER NOT NULL
            );"])?;

        // Create sync client with in-memory storage
        let config = SyncConfig::default();
        let sync_client = SyncClient::new_with_memory(config);

        // Save some test data
        let entity = TestEntity {
            name: "test-entity".to_string(),
            value: 42,
            ..Default::default()
        };
        let saved_entity = db.save(&entity)?;

        // Perform sync (should push our changes)
        sync_client.sync(&db)?;

        // Verify changes were pushed to storage
        let author_prefix = format!("authors/{}/", db.get_author());
        let files = sync_client.storage.list(&author_prefix)?;
        assert!(!files.is_empty());

        // Verify the change bundle contains our entity
        let file_content_bytes = sync_client.storage.get(&files[0])?;
        let file_content = String::from_utf8(file_content_bytes)
            .map_err(|e| anyhow::anyhow!("Failed to decode UTF-8: {}", e))?;
        let bundle: ChangeBundle = serde_json::from_str(&file_content)?;
        assert_eq!(bundle.author, db.get_author());
        assert_eq!(bundle.changes.len(), 1);
        assert_eq!(bundle.changes[0].entity_type, "TestEntity");
        assert_eq!(bundle.changes[0].entity_key, saved_entity.key);

        Ok(())
    }

    #[test]
    fn test_sync_between_two_databases() -> Result<()> {
        // Initialize debug logging for tests
        let _ = env_logger::Builder::from_default_env()
            .filter_level(log::LevelFilter::Debug)
            .is_test(true)
            .try_init();

        // Create two databases with the same schema
        let db1 = crate::db::Db::open_memory()?;
        let db2 = crate::db::Db::open_memory()?;

        let migration = "CREATE TABLE TestEntity (
            key TEXT NOT NULL PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER NOT NULL
        );";

        db1.migrate(&[migration])?;
        db2.migrate(&[migration])?;

        // Create sync clients sharing the same in-memory storage
        let config = SyncConfig::default();
        let sync_client1 = SyncClient::new_with_memory(config.clone());
        let sync_client2 = SyncClient::new_with_memory(config);

        // Create a shared storage for both sync clients
        let shared_storage = InMemoryStorage::new();
        let sync_client1 = SyncClient {
            config: sync_client1.config,
            storage: Box::new(shared_storage.clone()),
        };
        let sync_client2 = SyncClient {
            config: sync_client2.config,
            storage: Box::new(shared_storage.clone()),
        };

        // Add entity to db1
        let entity1 = TestEntity {
            name: "entity-from-db1".to_string(),
            value: 100,
            ..Default::default()
        };
        let saved_entity1 = db1.save(&entity1)?;

        // Sync db1 (pushes changes to shared storage)
        sync_client1.sync(&db1)?;

        // Add entity to db2
        let entity2 = TestEntity {
            name: "entity-from-db2".to_string(),
            value: 200,
            ..Default::default()
        };
        let saved_entity2 = db2.save(&entity2)?;

        // Sync db2 (should pull changes from db1 and push its own changes)
        sync_client2.sync(&db2)?;

        // Sync db1 again (should pull changes from db2)
        sync_client1.sync(&db1)?;

        // Verify both databases now have both entities
        let entities_in_db1: Vec<TestEntity> =
            db1.query("SELECT * FROM TestEntity ORDER BY name", &[])?;
        let entities_in_db2: Vec<TestEntity> =
            db2.query("SELECT * FROM TestEntity ORDER BY name", &[])?;

        assert_eq!(entities_in_db1.len(), 2);
        assert_eq!(entities_in_db2.len(), 2);

        // Check that both entities exist in both databases
        assert_eq!(entities_in_db1[0].name, "entity-from-db1");
        assert_eq!(entities_in_db1[0].key, saved_entity1.key);
        assert_eq!(entities_in_db1[1].name, "entity-from-db2");
        assert_eq!(entities_in_db1[1].key, saved_entity2.key);

        assert_eq!(entities_in_db2[0].name, "entity-from-db1");
        assert_eq!(entities_in_db2[0].key, saved_entity1.key);
        assert_eq!(entities_in_db2[1].name, "entity-from-db2");
        assert_eq!(entities_in_db2[1].key, saved_entity2.key);

        Ok(())
    }

    #[test]
    fn test_sync_change_ordering() -> Result<()> {
        // Initialize debug logging for tests
        let _ = env_logger::Builder::from_default_env()
            .filter_level(log::LevelFilter::Debug)
            .is_test(true)
            .try_init();

        let db1 = crate::db::Db::open_memory()?;
        let db2 = crate::db::Db::open_memory()?;

        let migration = "CREATE TABLE TestEntity (
            key TEXT NOT NULL PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER NOT NULL
        );";

        db1.migrate(&[migration])?;
        db2.migrate(&[migration])?;

        // Create sync clients with shared storage
        let shared_storage = InMemoryStorage::new();
        let config = SyncConfig::default();
        let sync_client1 = SyncClient {
            config: config.clone(),
            storage: Box::new(shared_storage.clone()),
        };
        let sync_client2 = SyncClient {
            config,
            storage: Box::new(shared_storage.clone()),
        };

        // Create entity in db1
        let entity = TestEntity {
            name: "test-entity".to_string(),
            value: 1,
            ..Default::default()
        };
        let saved_entity = db1.save(&entity)?;

        // Update the same entity multiple times in db1
        let updated_entity = TestEntity {
            key: saved_entity.key.clone(),
            name: "updated-entity".to_string(),
            value: 2,
        };
        db1.save(&updated_entity)?;

        let final_entity = TestEntity {
            key: saved_entity.key.clone(),
            name: "final-entity".to_string(),
            value: 3,
        };
        db1.save(&final_entity)?;

        // Sync to push all changes
        sync_client1.sync(&db1)?;

        // Sync db2 to pull changes
        sync_client2.sync(&db2)?;

        // Verify db2 has the final state
        let entities_in_db2: Vec<TestEntity> = db2.query(
            "SELECT * FROM TestEntity WHERE key = ?",
            &[&saved_entity.key],
        )?;
        assert_eq!(entities_in_db2.len(), 1);
        assert_eq!(entities_in_db2[0].name, "final-entity");
        assert_eq!(entities_in_db2[0].value, 3);

        Ok(())
    }

    #[test]
    fn test_sync_skip_own_changes() -> Result<()> {
        // Initialize debug logging for tests
        let _ = env_logger::Builder::from_default_env()
            .filter_level(log::LevelFilter::Debug)
            .is_test(true)
            .try_init();

        let db = crate::db::Db::open_memory()?;
        db.migrate(&["CREATE TABLE TestEntity (
                key TEXT NOT NULL PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER NOT NULL
            );"])?;

        let config = SyncConfig::default();
        let sync_client = SyncClient::new_with_memory(config);

        // Add entity and sync
        let entity = TestEntity {
            name: "test-entity".to_string(),
            value: 42,
            ..Default::default()
        };
        db.save(&entity)?;
        sync_client.sync(&db)?;

        // Count entities before second sync
        let entities_before: Vec<TestEntity> = db.query("SELECT * FROM TestEntity", &[])?;
        let changes_before = db.get_all_changes()?;

        // Sync again - should not duplicate entities
        sync_client.sync(&db)?;

        // Count entities after second sync
        let entities_after: Vec<TestEntity> = db.query("SELECT * FROM TestEntity", &[])?;
        let changes_after = db.get_all_changes()?;

        // Should have same number of entities (no duplicates)
        assert_eq!(entities_before.len(), entities_after.len());
        assert_eq!(entities_before.len(), 1);

        // Should not have applied any new changes (skipped own changes)
        assert_eq!(changes_before.len(), changes_after.len());

        Ok(())
    }

    #[test]
    fn test_s3_storage_creation() -> Result<()> {
        // Test that S3Storage can be created (doesn't test actual S3 operations)
        let result = S3Storage::new(
            "https://s3.amazonaws.com",
            "test-bucket",
            "us-east-1",
            "test-access-key",
            "test-secret-key",
        );

        // Should succeed in creating the storage instance
        assert!(result.is_ok());
        Ok(())
    }
}
