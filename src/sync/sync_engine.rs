use std::collections::HashSet;

use anyhow::Result;
use rmpv::Value as MsgPackValue;

use crate::{changelog::{BasicStorageChangelog, Changelog}, storage::{EncryptedStorage, InMemoryStorage, LocalStorage, S3Storage, SyncStorage}, Db};

pub struct SyncEngine {
    storage: Box<dyn SyncStorage>,
    prefix: String,
}

pub struct GenericSyncEngine;

impl GenericSyncEngine {
    /// Sync algorithm that works with any two Changelog implementations
    /// 
    /// The goal is for every device/replica/author to have a complete copy of
    /// the changelog. From the changelog we can replicate the entity state
    /// at any point in time from the perspective of any author.
    /// 
    /// 1. Get the sets of local and remote change_ids.
    /// 2. For any remote change_id not in the local set, download and insert
    /// it, setting merged = false. 
    /// 3. For any local change_id not in the remote set, upload it.
    /// 
    /// Call changelogs to merge entity updates.
    pub fn sync(local: &dyn Changelog, remote: &dyn Changelog) -> Result<()> {
        // 1. Get the sets of local and remote change_ids.
        log::info!("Sync: Getting change lists.");
        let local_change_ids = local.get_all_change_ids()?
            .into_iter().collect::<HashSet<_>>();
        let remote_change_ids = remote.get_all_change_ids()?
            .into_iter().collect::<HashSet<_>>();

        log::info!("Sync: Syncing {} local and {} remote changes.", 
            local_change_ids.len(), remote_change_ids.len());

        // 2. For any remote change_id not in the local set, download and append it
        let change_ids_to_pull = remote_change_ids.iter()
            .filter(|id| !local_change_ids.contains(*id))
            .collect::<Vec<_>>();
        log::info!("Sync: Pulling {} new changes.", change_ids_to_pull.len());
        let pull_min = change_ids_to_pull.iter().min().cloned().map(|s| s.as_str());
        let pull_max = change_ids_to_pull.iter().max().cloned().map(|s| s.as_str());
        let pulled_changes = remote.get_changes(pull_min, pull_max)?;
        local.append_changes(pulled_changes)?;
        
        // 3. For any local change_id not in the remote set, upload it
        let change_ids_to_push = local_change_ids.iter()
            .filter(|id| !remote_change_ids.contains(*id))
            .collect::<Vec<_>>();
        log::info!("Sync: Pushing {} new changes.", change_ids_to_push.len());
        let push_min = change_ids_to_push.iter().min().cloned().map(|s| s.as_str());
        let push_max = change_ids_to_push.iter().max().cloned().map(|s| s.as_str());
        let changes_to_push = local.get_changes(push_min, push_max)?;
        remote.append_changes(changes_to_push)?;

        log::info!("Sync: Done. =============");
        Ok(())
    }
}

impl SyncEngine {
    pub fn new_with_storage(storage: Box<dyn SyncStorage>, prefix: String) -> Result<Self> {
        Ok(SyncEngine {
            storage,
            prefix,
        })
    }

    pub fn builder() -> SyncEngineBuilder {
        SyncEngineBuilder::default()
    }


    /// Sync using the generic sync algorithm with DbChangelog and BatchingStorageChangelog
    pub fn sync(&self, db: &Db) -> Result<()> {
        use crate::changelog::{DbChangelog};
        
        let local_changelog = DbChangelog::new(db.clone());
        let remote_changelog = BasicStorageChangelog::new(self.storage.as_ref(), self.prefix.clone());
        
        // Use the generic sync algorithm
        Ok(GenericSyncEngine::sync(&local_changelog, &remote_changelog)?)
    }

}

/// Convert a rusqlite::Value to a MessagePack Value
pub fn sql_value_to_msgpack(value: &rusqlite::types::Value) -> MsgPackValue {
    match value {
        rusqlite::types::Value::Null => MsgPackValue::Nil,
        rusqlite::types::Value::Integer(i) => MsgPackValue::Integer((*i).into()),
        rusqlite::types::Value::Real(f) => MsgPackValue::F64(*f),
        rusqlite::types::Value::Text(s) => MsgPackValue::String(s.clone().into()),
        rusqlite::types::Value::Blob(b) => MsgPackValue::Binary(b.clone()),
    }
}

/// Convert a MessagePack Value back to a rusqlite::Value
pub fn msgpack_to_sql_value(value: &MsgPackValue) -> rusqlite::types::Value {
    match value {
        MsgPackValue::Nil => rusqlite::types::Value::Null,
        MsgPackValue::Boolean(b) => rusqlite::types::Value::Integer(*b as i64),
        MsgPackValue::Integer(i) => {
            if let Some(i64_val) = i.as_i64() {
                rusqlite::types::Value::Integer(i64_val)
            } else if let Some(u64_val) = i.as_u64() {
                rusqlite::types::Value::Integer(u64_val as i64)
            } else {
                rusqlite::types::Value::Null
            }
        },
        MsgPackValue::F32(f) => rusqlite::types::Value::Real(*f as f64),
        MsgPackValue::F64(f) => rusqlite::types::Value::Real(*f),
        MsgPackValue::String(s) => {
            if let Some(string) = s.as_str() {
                rusqlite::types::Value::Text(string.to_string())
            } else {
                rusqlite::types::Value::Null
            }
        },
        MsgPackValue::Binary(b) => rusqlite::types::Value::Blob(b.clone()),
        _ => rusqlite::types::Value::Null,
    }
}

#[derive(Default)]
pub struct SyncEngineBuilder {
    storage: Option<Box<dyn SyncStorage>>,
    passphrase: Option<String>,
    prefix: Option<String>,
}

impl SyncEngineBuilder {
    pub fn in_memory(mut self) -> Self {
        self.storage = Some(Box::new(InMemoryStorage::new()));
        self
    }

    pub fn local(mut self, base_path: &str) -> Self {
        self.storage = Some(Box::new(LocalStorage::new(base_path)));
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

    pub fn prefix(mut self, prefix: &str) -> Self {
        self.prefix = Some(prefix.to_string());
        self
    }

    pub fn build(self) -> Result<SyncEngine> {
        let prefix = self.prefix.unwrap_or_else(|| "dimple-sync".to_string());
        
        if let Some(passphrase) = self.passphrase {
            let storage = EncryptedStorage::new(self.storage.unwrap(), passphrase);
            SyncEngine::new_with_storage(Box::new(storage), prefix)
        }
        else {
            SyncEngine::new_with_storage(self.storage.unwrap(), prefix)
        }
    }
}

#[cfg(test)]
mod tests {
    use rusqlite_migration::{Migrations, M};
    use serde::{Deserialize, Serialize};
    use crate::{changelog::ChangelogChange, db::DbEvent, sync::SyncEngine, Db};

    #[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
    struct Artist {
        pub id: String,
        pub name: String,
        pub country: Option<String>,
        pub summary: Option<String>,
        pub liked: Option<bool>,
    }

    #[test]
    fn basic_sync() -> anyhow::Result<()> {
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (name TEXT NOT NULL, country TEXT, id TEXT NOT NULL PRIMARY KEY);"),
        ]);
        let db1 = Db::open_memory()?;
        let db2 = Db::open_memory()?;
        db1.migrate(&migrations)?;
        db2.migrate(&migrations)?;
        
        db1.save(&Artist {
            name: "Metallica".to_string(),
            ..Default::default()
        })?;
        db1.save(&Artist {
            name: "Megadeth".to_string(),
            ..Default::default()
        })?;
        db1.save(&Artist {
            ..Default::default()
        })?;
        db2.save(&Artist {
            name: "Anthrax".to_string(),
            ..Default::default()
        })?;
        db2.save(&Artist {
            ..Default::default()
        })?;
        
        let sync_engine = SyncEngine::builder()
            .in_memory()
            // .encrypted("correct horse battery staple")
            .build()?;
            
        sync_engine.sync(&db1)?;
        sync_engine.sync(&db2)?;
        sync_engine.sync(&db1)?;
        sync_engine.sync(&db2)?;
        
        assert_eq!(db1.query::<Artist, _>("SELECT * FROM Artist", ())?.len(), 5);
        assert_eq!(db2.query::<Artist, _>("SELECT * FROM Artist", ())?.len(), 5);
        Ok(())
    }

    /// Ensures replicas will "catch up" when A has synced multiple times since
    /// B.
    #[test]
    fn catchup() -> anyhow::Result<()> {
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (name TEXT NOT NULL, country TEXT, id TEXT NOT NULL PRIMARY KEY);"),
        ]);
        let db1 = Db::open_memory()?;
        let db2 = Db::open_memory()?;
        db1.migrate(&migrations)?;
        db2.migrate(&migrations)?;

        let sync_engine = SyncEngine::builder()
            .in_memory()
            .build()?;
        db1.save(&Artist {
            name: "Metallica".to_string(),
            ..Default::default()
        })?;
        sync_engine.sync(&db1)?;

        db1.save(&Artist {
            name: "Megadeth".to_string(),
            ..Default::default()
        })?;
        sync_engine.sync(&db1)?;

        db1.save(&Artist {
            name: "Anthrax".to_string(),
            ..Default::default()
        })?;
        db1.save(&Artist {
            name: "Slayer".to_string(),
            ..Default::default()
        })?;
        sync_engine.sync(&db1)?;
        
        sync_engine.sync(&db2)?;
        
        assert_eq!(db1.query::<Artist, _>("SELECT * FROM Artist", ())?.len(), 4);
        assert_eq!(db2.query::<Artist, _>("SELECT * FROM Artist", ())?.len(), 4);
        Ok(())
    }

    #[test]
    fn lww_per_attribute() -> anyhow::Result<()> {
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, country TEXT, summary TEXT, liked BOOL);"),
        ]);

        let db1 = Db::open_memory()?;
        let db2 = Db::open_memory()?;
        let db3 = Db::open_memory()?;
        
        db1.migrate(&migrations)?;
        db2.migrate(&migrations)?;
        db3.migrate(&migrations)?;

        let sync_engine = SyncEngine::builder()
            .in_memory()
            .build()?;

        // Device A creates an artist
        let artist_a = db1.save(&Artist {
            name: "Iron Maiden".to_string(),
            ..Default::default()
        })?;

        // All devices sync
        sync_engine.sync(&db1)?;
        sync_engine.sync(&db2)?;
        sync_engine.sync(&db3)?;

        // Device B changes country, summary, and liked
        let mut artist_b: Artist = db2.get(&artist_a.id)?.unwrap();
        artist_b.country = Some("UK".to_string());
        artist_b.summary = Some("Rock Gods".to_string());
        artist_b.liked = Some(false);
        db2.save(&artist_b)?;

        // Device C changes summary and liked
        let mut artist_c: Artist = db3.get(&artist_a.id)?.unwrap();
        artist_c.summary = Some("Rock Legends".to_string());
        artist_c.liked = Some(true);
        db3.save(&artist_c)?;

        // Everyone syncs
        sync_engine.sync(&db1)?;
        sync_engine.sync(&db2)?;
        sync_engine.sync(&db3)?;
        sync_engine.sync(&db1)?;
        sync_engine.sync(&db2)?;
        sync_engine.sync(&db3)?;

        let final_artist_a: Artist = db1.get(&artist_a.id)?.unwrap();
        let final_artist_b: Artist = db2.get(&artist_a.id)?.unwrap();
        let final_artist_c: Artist = db3.get(&artist_a.id)?.unwrap();

        // All replicas should have identical final state
        assert_eq!(final_artist_a.name, "Iron Maiden"); // From A
        assert_eq!(final_artist_a.country, Some("UK".to_string())); // From B
        assert_eq!(final_artist_a.summary, Some("Rock Legends".to_string())); // From C
        assert_eq!(final_artist_a.liked, Some(true)); // From C (overwrting B's change)

        assert_eq!(final_artist_a, final_artist_b);
        assert_eq!(final_artist_a, final_artist_c);
        
        Ok(())
    }

    #[test]
    fn test_sync_engine_prefix() -> anyhow::Result<()> {
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL);"),
        ]);
        
        let db1 = Db::open_memory()?;
        let db2 = Db::open_memory()?;
        db1.migrate(&migrations)?;
        db2.migrate(&migrations)?;
        
        // Create sync engine with custom prefix
        let sync_engine = SyncEngine::builder()
            .in_memory()
            .prefix("test-prefix-123")
            .build()?;
        
        // Add data to db1
        db1.save(&Artist {
            name: "Test Artist".to_string(),
            ..Default::default()
        })?;
        
        // Sync to storage
        sync_engine.sync(&db1)?;
        
        // Verify that the storage contains files with the prefix (now using buckets)
        let bucket_files = sync_engine.storage.list("test-prefix-123/buckets/")?;
        assert!(!bucket_files.is_empty(), "No bucket files found with prefix");
        assert!(bucket_files.iter().all(|f| f.starts_with("test-prefix-123/buckets/")), 
                "Files don't have correct prefix: {:?}", bucket_files);
        
        // Sync to db2 to verify it works end-to-end
        sync_engine.sync(&db2)?;
        
        let artists2: Vec<Artist> = db2.query("SELECT * FROM Artist", ())?;
        assert_eq!(artists2.len(), 1);
        assert_eq!(artists2[0].name, "Test Artist");
        
        Ok(())
    }

    #[test]
    fn test_sync_engine_default_prefix() -> anyhow::Result<()> {
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL);"),
        ]);
        
        let db = Db::open_memory()?;
        db.migrate(&migrations)?;
        
        // Create sync engine without specifying prefix (should use default)
        let sync_engine = SyncEngine::builder()
            .in_memory()
            .build()?;
        
        // Add data
        db.save(&Artist {
            name: "Default Prefix Test".to_string(),
            ..Default::default()
        })?;
        
        // Sync to storage
        sync_engine.sync(&db)?;
        
        // Verify that the storage contains files with the default prefix (now using buckets)
        let bucket_files = sync_engine.storage.list("dimple-sync/buckets/")?;
        assert!(!bucket_files.is_empty(), "No bucket files found with default prefix");
        assert!(bucket_files.iter().all(|f| f.starts_with("dimple-sync/buckets/")), 
                "Files don't have correct default prefix: {:?}", bucket_files);
        
        Ok(())
    }

    #[test]
    fn test_newer_local_changes_not_overwritten() -> anyhow::Result<()> {
        // Test that syncing doesn't overwrite newer local changes with older remote changes
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL, country TEXT);"),
        ]);
        
        let db_a = Db::open_memory()?;
        let db_b = Db::open_memory()?;
        db_a.migrate(&migrations)?;
        db_b.migrate(&migrations)?;
        
        let sync_engine = SyncEngine::builder()
            .in_memory()
            .build()?;
        
        // Step 1: Device A creates an artist
        let artist = db_a.save(&Artist {
            name: "The Beatles".to_string(),
            ..Default::default()
        })?;
        
        // Step 2: Both devices sync - now both have the artist
        sync_engine.sync(&db_a)?;
        sync_engine.sync(&db_b)?;
        
        // Step 3: Device A updates the country to "UK"
        let mut artist_a: Artist = db_a.get(&artist.id)?.unwrap();
        artist_a.country = Some("UK".to_string());
        db_a.save(&artist_a)?;
        
        // Step 4: Device A syncs (uploads the change)
        sync_engine.sync(&db_a)?;
        
        // Step 5: BEFORE syncing, Device B updates the same field to "England" 
        // This creates a NEWER change than A's "UK" change
        let mut artist_b: Artist = db_b.get(&artist.id)?.unwrap();
        artist_b.country = Some("England".to_string());
        db_b.save(&artist_b)?;
        
        // Step 6: Device B syncs
        // The sync should NOT overwrite B's newer "England" value with A's older "UK" value
        sync_engine.sync(&db_b)?;
        
        // Verify that Device B still has "England" (the newer value)
        let final_artist_b: Artist = db_b.get(&artist.id)?.unwrap();
        assert_eq!(final_artist_b.country, Some("England".to_string()), 
                   "Device B's newer change was overwritten by older remote change!");
        
        // Debug: Check the change records in device B
        let changes_b: Vec<ChangelogChange> = db_b.query(
            "SELECT * FROM ZV_CHANGE WHERE entity_id = ? ORDER BY id",
            [&artist.id]
        )?;
        println!("Device B changes after sync:");
        for change in &changes_b {
            println!("  Change {}: merged={}", 
                     change.id, change.merged);
        }
        
        // After another sync cycle, both should converge to "England" (the newest value)
        sync_engine.sync(&db_a)?;
        let final_artist_a: Artist = db_a.get(&artist.id)?.unwrap();
        assert_eq!(final_artist_a.country, Some("England".to_string()),
                   "Devices did not converge to the newest value");
        
        Ok(())
    }
    
    #[test]
    fn test_merged_local_changes_not_overwritten() -> anyhow::Result<()> {
        // Test a more specific scenario where local changes are already merged
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL, country TEXT);"),
        ]);
        
        let db_a = Db::open_memory()?;
        let db_b = Db::open_memory()?;
        db_a.migrate(&migrations)?;
        db_b.migrate(&migrations)?;
        
        let sync_engine = SyncEngine::builder()
            .in_memory()
            .build()?;
        
        // Device A creates an artist
        let artist = db_a.save(&Artist {
            name: "The Beatles".to_string(),
            ..Default::default()
        })?;
        
        // Both devices sync
        sync_engine.sync(&db_a)?;
        sync_engine.sync(&db_b)?;
        
        // Device A updates country to "UK" but DOESN'T sync yet
        let mut artist_a: Artist = db_a.get(&artist.id)?.unwrap();
        artist_a.country = Some("UK".to_string());
        db_a.save(&artist_a)?;
        
        // Device B updates country to "England" and DOES sync
        let mut artist_b: Artist = db_b.get(&artist.id)?.unwrap();
        artist_b.country = Some("England".to_string());
        db_b.save(&artist_b)?;
        sync_engine.sync(&db_b)?;  // This marks B's change as merged
        
        // Now Device A syncs its older "UK" change
        sync_engine.sync(&db_a)?;
        
        // Device B syncs again - should NOT overwrite "England" with "UK"
        sync_engine.sync(&db_b)?;
        
        let final_artist_b: Artist = db_b.get(&artist.id)?.unwrap();
        assert_eq!(final_artist_b.country, Some("England".to_string()), 
                   "Device B's already-merged newer change was overwritten!");
        
        Ok(())
    }

    #[test]
    fn foreign_key_ordering() -> anyhow::Result<()> {
        #[derive(Serialize, Deserialize, Debug, Default)]
        struct Artist {
            id: String,
            name: String,
        }
        
        #[derive(Serialize, Deserialize, Debug, Default)]
        struct Album {
            id: String,
            title: String,
            artist_id: String,
        }
        
        // Test that entities with foreign keys are synced after their dependencies
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL);"),
            M::up("CREATE TABLE Album (id TEXT PRIMARY KEY, title TEXT NOT NULL, artist_id TEXT NOT NULL, 
                   FOREIGN KEY (artist_id) REFERENCES Artist(id));"),
        ]);
        
        let db1 = Db::open_memory()?;
        let db2 = Db::open_memory()?;
        db1.migrate(&migrations)?;
        db2.migrate(&migrations)?;
        
        // Create an artist and album in db1
        let artist = db1.save(&Artist {
            name: "The Beatles".to_string(),
            ..Default::default()
        })?;
        
        let album = db1.save(&Album {
            title: "Abbey Road".to_string(),
            artist_id: artist.id.clone(),
            ..Default::default()
        })?;
        
        let sync_engine = SyncEngine::builder()
            .in_memory()
            .build()?;
            
        // Sync to storage
        sync_engine.sync(&db1)?;
        
        // Sync from storage to db2 - this should not fail with foreign key constraint
        // even though the HashMap iteration order might try to insert Album before Artist
        sync_engine.sync(&db2)?;
        
        // Verify both entities exist in db2
        let artists: Vec<Artist> = db2.query(
            "SELECT * FROM Artist WHERE id = ?", 
            [&artist.id]
        )?;
        assert_eq!(artists.len(), 1);
        assert_eq!(artists[0].name, "The Beatles");
        
        let albums: Vec<Album> = db2.query(
            "SELECT * FROM Album WHERE id = ?", 
            [&album.id]
        )?;
        assert_eq!(albums.len(), 1);
        assert_eq!(albums[0].title, "Abbey Road");
        assert_eq!(albums[0].artist_id, artist.id);
        
        Ok(())
    }

    #[test]
    fn test_sync_triggers_notifications() -> anyhow::Result<()> {
        use std::time::Duration;
        
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL, country TEXT);"),
        ]);
        
        let db1 = Db::open_memory()?;
        let db2 = Db::open_memory()?;
        db1.migrate(&migrations)?;
        db2.migrate(&migrations)?;
        
        let sync_engine = SyncEngine::builder()
            .in_memory()
            .build()?;
        
        // Create an artist in db1
        let artist = db1.save(&Artist {
            name: "Pink Floyd".to_string(),
            country: Some("UK".to_string()),
            ..Default::default()
        })?;
        
        // Sync db1 to storage
        sync_engine.sync(&db1)?;
        
        // Subscribe to notifications on db2
        let receiver = db2.subscribe();
        
        // Sync db2 from storage - this should trigger a notification
        sync_engine.sync(&db2)?;
        
        // Check that we received an insert notification
        let event = receiver.recv_timeout(Duration::from_secs(1))?;
        match event {
            DbEvent::Insert(entity_type, entity_id) => {
                assert_eq!(entity_type, "Artist");
                assert_eq!(entity_id, artist.id);
            }
            _ => panic!("Expected Insert event, got {:?}", event),
        }
        
        // Now update the artist in db1
        let mut updated_artist = artist.clone();
        updated_artist.country = Some("United Kingdom".to_string());
        db1.save(&updated_artist)?;
        
        // Sync changes
        sync_engine.sync(&db1)?;
        
        // Sync db2 again - should get an update notification
        sync_engine.sync(&db2)?;
        
        // Check for update notification
        let event = receiver.recv_timeout(Duration::from_secs(1))?;
        match event {
            DbEvent::Update(entity_type, entity_id) => {
                assert_eq!(entity_type, "Artist");
                assert_eq!(entity_id, artist.id);
            }
            _ => panic!("Expected Update event, got {:?}", event),
        }
        
        Ok(())
    }

    #[test]
    fn test_generic_sync_engine() -> anyhow::Result<()> {
        use crate::{changelog::{DbChangelog, BatchingStorageChangelog}, storage::InMemoryStorage};
        use super::GenericSyncEngine;
        
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL, country TEXT);"),
        ]);
        
        let db1 = Db::open_memory()?;
        let db2 = Db::open_memory()?;
        db1.migrate(&migrations)?;
        db2.migrate(&migrations)?;
        
        let storage = InMemoryStorage::new();
        
        // Create changelogs
        let db1_changelog = DbChangelog::new(db1.clone());
        let db2_changelog = DbChangelog::new(db2.clone());
        let storage_changelog = BatchingStorageChangelog::new(&storage, "test".to_string());
        
        // Add data to db1
        let _artist1 = db1.save(&Artist {
            name: "The Beatles".to_string(),
            ..Default::default()
        })?;
        
        // Add data to db2
        let _artist2 = db2.save(&Artist {
            name: "Pink Floyd".to_string(),
            ..Default::default()
        })?;
        
        // Sync db1 to storage
        GenericSyncEngine::sync(&db1_changelog, &storage_changelog)?;
        
        // Sync db2 to storage  
        GenericSyncEngine::sync(&db2_changelog, &storage_changelog)?;
        
        // Sync storage back to db1 (should get Pink Floyd)
        GenericSyncEngine::sync(&storage_changelog, &db1_changelog)?;
        crate::changelog::merge_unmerged_changes(&db1)?;
        
        // Sync storage back to db2 (should get The Beatles)
        GenericSyncEngine::sync(&storage_changelog, &db2_changelog)?;
        crate::changelog::merge_unmerged_changes(&db2)?;
        
        // Both databases should now have both artists
        let artists1: Vec<Artist> = db1.query("SELECT * FROM Artist ORDER BY name", ())?;
        let artists2: Vec<Artist> = db2.query("SELECT * FROM Artist ORDER BY name", ())?;
        
        assert_eq!(artists1.len(), 2);
        assert_eq!(artists2.len(), 2);
        assert_eq!(artists1[0].name, "Pink Floyd");
        assert_eq!(artists1[1].name, "The Beatles");
        assert_eq!(artists2[0].name, "Pink Floyd");
        assert_eq!(artists2[1].name, "The Beatles");
        
        Ok(())
    }

    #[test]
    fn performance_comparison_small_dataset() -> anyhow::Result<()> {
        use crate::{changelog::{Changelog, DbChangelog, BatchingStorageChangelog, BasicStorageChangelog}, storage::SlowInMemoryStorage};
        use super::GenericSyncEngine;
        use std::time::Instant;
        
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL, country TEXT);"),
        ]);
        
        // Create test databases
        let db1 = Db::open_memory()?;
        let db2 = Db::open_memory()?;
        db1.migrate(&migrations)?;
        db2.migrate(&migrations)?;
        
        // Add test data (20 records)
        let num_records = 20;
        for i in 0..num_records {
            db1.save(&Artist {
                name: format!("Artist {}", i),
                country: Some("Test Country".to_string()),
                ..Default::default()
            })?;
        }
        
        let storage = SlowInMemoryStorage::new(1, 10, 2); // Fast testing: GET: 1ms, PUT: 10ms, LIST: 2ms
        let db_changelog = DbChangelog::new(db1.clone());
        
        // Test BatchingStorageChangelog
        let batching_changelog = BatchingStorageChangelog::new(&storage, "batching".to_string());
        let start = Instant::now();
        GenericSyncEngine::sync(&db_changelog, &batching_changelog)?;
        let batching_duration = start.elapsed();
        
        // Test BasicStorageChangelog
        let basic_changelog = BasicStorageChangelog::new(&storage, "basic".to_string());
        let start = Instant::now();
        GenericSyncEngine::sync(&db_changelog, &basic_changelog)?;
        let basic_duration = start.elapsed();
        
        println!("Small dataset ({} records):", num_records);
        println!("  Batching approach: {:?}", batching_duration);
        println!("  Basic approach:    {:?}", basic_duration);
        println!("  Ratio: {:.2}x", basic_duration.as_nanos() as f64 / batching_duration.as_nanos() as f64);
        
        // Verify both approaches stored the same number of changes
        assert_eq!(batching_changelog.get_all_change_ids()?.len(), num_records);
        assert_eq!(basic_changelog.get_all_change_ids()?.len(), num_records);
        
        Ok(())
    }

    #[test]
    fn performance_comparison_large_dataset() -> anyhow::Result<()> {
        use crate::{changelog::{Changelog, DbChangelog, BatchingStorageChangelog, BasicStorageChangelog}, storage::SlowInMemoryStorage};
        use super::GenericSyncEngine;
        use std::time::Instant;
        
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL, country TEXT);"),
        ]);
        
        // Create test databases
        let db1 = Db::open_memory()?;
        db1.migrate(&migrations)?;
        
        // Add test data (30 records - larger dataset)
        let num_records = 30;
        for i in 0..num_records {
            db1.save(&Artist {
                name: format!("Artist {}", i),
                country: Some(format!("Country {}", i % 10)),
                ..Default::default()
            })?;
        }
        
        let storage = SlowInMemoryStorage::new(1, 10, 2); // Fast testing: GET: 1ms, PUT: 10ms, LIST: 2ms
        let db_changelog = DbChangelog::new(db1.clone());
        
        // Test BatchingStorageChangelog
        let batching_changelog = BatchingStorageChangelog::new(&storage, "batching".to_string());
        let start = Instant::now();
        GenericSyncEngine::sync(&db_changelog, &batching_changelog)?;
        let batching_duration = start.elapsed();
        
        // Test BasicStorageChangelog
        let basic_changelog = BasicStorageChangelog::new(&storage, "basic".to_string());
        let start = Instant::now();
        GenericSyncEngine::sync(&db_changelog, &basic_changelog)?;
        let basic_duration = start.elapsed();
        
        println!("Large dataset ({} records):", num_records);
        println!("  Batching approach: {:?}", batching_duration);
        println!("  Basic approach:    {:?}", basic_duration);
        println!("  Ratio: {:.2}x", basic_duration.as_nanos() as f64 / batching_duration.as_nanos() as f64);
        
        // Verify both approaches stored the same number of changes
        assert_eq!(batching_changelog.get_all_change_ids()?.len(), num_records);
        assert_eq!(basic_changelog.get_all_change_ids()?.len(), num_records);
        
        Ok(())
    }

    #[test]
    fn performance_comparison_incremental_sync() -> anyhow::Result<()> {
        use crate::{changelog::{DbChangelog, BatchingStorageChangelog, BasicStorageChangelog}, storage::SlowInMemoryStorage};
        use super::GenericSyncEngine;
        use std::time::Instant;
        
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL, country TEXT);"),
        ]);
        
        // Create test databases
        let db1 = Db::open_memory()?;
        let db2 = Db::open_memory()?;
        db1.migrate(&migrations)?;
        db2.migrate(&migrations)?;
        
        // Add initial data to both databases
        let initial_records = 15;
        for i in 0..initial_records {
            db1.save(&Artist {
                name: format!("Initial Artist {}", i),
                ..Default::default()
            })?;
            db2.save(&Artist {
                name: format!("Initial Artist {}", i),
                ..Default::default()
            })?;
        }
        
        let storage = SlowInMemoryStorage::new(1, 10, 2); // Fast testing: GET: 1ms, PUT: 10ms, LIST: 2ms
        let db1_changelog = DbChangelog::new(db1.clone());
        let db2_changelog = DbChangelog::new(db2.clone());
        
        // Do initial sync with both approaches
        let batching_changelog = BatchingStorageChangelog::new(&storage, "batching".to_string());
        let basic_changelog = BasicStorageChangelog::new(&storage, "basic".to_string());
        
        GenericSyncEngine::sync(&db1_changelog, &batching_changelog)?;
        GenericSyncEngine::sync(&db1_changelog, &basic_changelog)?;
        
        // Add incremental changes to db2
        let incremental_records = 5;
        for i in 0..incremental_records {
            db2.save(&Artist {
                name: format!("New Artist {}", i),
                country: Some("New Country".to_string()),
                ..Default::default()
            })?;
        }
        
        // Test incremental sync performance
        let start = Instant::now();
        GenericSyncEngine::sync(&db2_changelog, &batching_changelog)?;
        let batching_incremental = start.elapsed();
        
        let start = Instant::now();
        GenericSyncEngine::sync(&db2_changelog, &basic_changelog)?;
        let basic_incremental = start.elapsed();
        
        println!("Incremental sync ({} new records on top of {}):", incremental_records, initial_records);
        println!("  Batching approach: {:?}", batching_incremental);
        println!("  Basic approach:    {:?}", basic_incremental);
        println!("  Ratio: {:.2}x", basic_incremental.as_nanos() as f64 / batching_incremental.as_nanos() as f64);
        
        Ok(())
    }
}