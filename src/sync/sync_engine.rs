use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::db::{Change, Db};
use super::sync_target::{SyncTarget, S3Storage, LocalStorage, InMemoryStorage, EncryptedStorage};

#[derive(Debug, Clone)]
pub struct SyncConfig {
    pub endpoint: String,
    pub bucket: String,
    pub base_path: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
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
            passphrase: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChangeBundle {
    pub author: String,
    pub changes: Vec<Change>,
}

pub struct SyncEngine {
    pub config: SyncConfig,
    pub target: Box<dyn SyncTarget>,
}

impl SyncEngine {
    pub fn new_with_s3(config: SyncConfig) -> Result<Self> {
        let target = S3Storage::new(
            &config.endpoint,
            &config.bucket,
            &config.region,
            &config.access_key,
            &config.secret_key,
        )?;

        let target: Box<dyn SyncTarget> = if let Some(passphrase) = &config.passphrase {
            Box::new(EncryptedStorage::new(Box::new(target), passphrase.clone()))
        } else {
            Box::new(target)
        };

        Ok(Self { config, target })
    }

    pub fn new_with_local(config: SyncConfig, base_path: &str) -> Self {
        let target = LocalStorage::new(base_path);
        
        let target: Box<dyn SyncTarget> = if let Some(passphrase) = &config.passphrase {
            Box::new(EncryptedStorage::new(Box::new(target), passphrase.clone()))
        } else {
            Box::new(target)
        };
        
        Self { config, target }
    }

    pub fn new_with_memory(config: SyncConfig) -> Self {
        let target = InMemoryStorage::new();
        
        let target: Box<dyn SyncTarget> = if let Some(passphrase) = &config.passphrase {
            Box::new(EncryptedStorage::new(Box::new(target), passphrase.clone()))
        } else {
            Box::new(target)
        };
        
        Self { config, target }
    }

    pub fn sync(&self, db: &Db) -> Result<()> {
        log::info!("Starting sync for author {}", db.get_database_uuid());

        // 1. Pull changes from other authors
        let remote_changes = self.pull_remote_changes(db.get_database_uuid(), db)?;

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
        let author_dirs = self.target.list(&authors_prefix)?;

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
        let mut change_files = self.target.list(author_path)?;

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
            let content_bytes = self.target.get(&file_path)?;
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
        let bundle = ChangeBundle {
            author: author.to_string(),
            changes: changes.to_vec(),
        };

        // Generate filename based on latest change transaction ID
        let latest_change_id = if let Some(last_change) = changes.last() {
            &last_change.transaction_id
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
        let content = serde_json::to_string_pretty(&bundle)?;
        self.target.put(&path, content.as_bytes())?;

        log::info!("Pushed changes to {}", path);
        Ok(())
    }


    fn get_latest_pushed_uuid(&self, author: &str) -> Result<String> {
        // List files for this author to find the latest pushed UUID
        let author_prefix = if self.config.base_path.is_empty() {
            format!("authors/{}/", author)
        } else {
            format!("{}/authors/{}/", self.config.base_path, author)
        };

        let mut files = self.target.list(&author_prefix)?;

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
        let author = db.get_database_uuid();

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
    }

    #[test]
    fn test_change_bundle_serialization() -> Result<()> {
        let bundle = ChangeBundle {
            author: "test-author".to_string(),
            changes: vec![],
        };

        let json = serde_json::to_string(&bundle)?;
        let deserialized: ChangeBundle = serde_json::from_str(&json)?;

        assert_eq!(bundle.author, deserialized.author);

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

        // Create sync engine with in-memory target
        let config = SyncConfig::default();
        let sync = SyncEngine::new_with_memory(config);

        // Save some test data
        let entity = TestEntity {
            name: "test-entity".to_string(),
            value: 42,
            ..Default::default()
        };
        let saved_entity = db.save(&entity)?;

        // Perform sync (should push our changes)
        sync.sync(&db)?;

        // Verify changes were pushed to storage
        let author_prefix = format!("authors/{}/", db.get_database_uuid());
        let files = sync.target.list(&author_prefix)?;
        assert!(!files.is_empty());

        // Verify the change bundle contains our entity
        let file_content_bytes = sync.target.get(&files[0])?;
        let file_content = String::from_utf8(file_content_bytes)
            .map_err(|e| anyhow::anyhow!("Failed to decode UTF-8: {}", e))?;
        let bundle: ChangeBundle = serde_json::from_str(&file_content)?;
        assert_eq!(bundle.author, db.get_database_uuid());
        assert_eq!(bundle.changes.len(), 3); // key, name, value attributes
        // Verify all changes are for the same entity
        for change in &bundle.changes {
            assert_eq!(change.entity_type, "TestEntity");
            assert_eq!(change.entity_key, saved_entity.key);
        }

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

        // Create sync engines sharing the same in-memory target
        let config = SyncConfig::default();
        let sync1 = SyncEngine::new_with_memory(config.clone());
        let sync2 = SyncEngine::new_with_memory(config);

        // Create a shared target for both sync engines
        let shared_target = InMemoryStorage::new();
        let sync1 = SyncEngine {
            config: sync1.config,
            target: Box::new(shared_target.clone()),
        };
        let sync2 = SyncEngine {
            config: sync2.config,
            target: Box::new(shared_target.clone()),
        };

        // Add entity to db1
        let entity1 = TestEntity {
            name: "entity-from-db1".to_string(),
            value: 100,
            ..Default::default()
        };
        let saved_entity1 = db1.save(&entity1)?;

        // Sync db1 (pushes changes to shared target)
        sync1.sync(&db1)?;

        // Add entity to db2
        let entity2 = TestEntity {
            name: "entity-from-db2".to_string(),
            value: 200,
            ..Default::default()
        };
        let saved_entity2 = db2.save(&entity2)?;

        // Sync db2 (should pull changes from db1 and push its own changes)
        sync2.sync(&db2)?;

        // Sync db1 again (should pull changes from db2)
        sync1.sync(&db1)?;

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

        // Create sync engines with shared target
        let shared_target = InMemoryStorage::new();
        let config = SyncConfig::default();
        let sync1 = SyncEngine {
            config: config.clone(),
            target: Box::new(shared_target.clone()),
        };
        let sync2 = SyncEngine {
            config,
            target: Box::new(shared_target.clone()),
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
        sync1.sync(&db1)?;

        // Sync db2 to pull changes
        sync2.sync(&db2)?;

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
        let sync = SyncEngine::new_with_memory(config);

        // Add entity and sync
        let entity = TestEntity {
            name: "test-entity".to_string(),
            value: 42,
            ..Default::default()
        };
        db.save(&entity)?;
        sync.sync(&db)?;

        // Count entities before second sync
        let entities_before: Vec<TestEntity> = db.query("SELECT * FROM TestEntity", &[])?;
        let changes_before = db.get_all_changes()?;

        // Sync again - should not duplicate entities
        sync.sync(&db)?;

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
}