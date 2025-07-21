use std::collections::{HashMap, HashSet};

use anyhow::Result;

use crate::{db::{ChangeRecord, Db, transaction::DbTransaction}, sync::{EncryptedStorage, InMemoryStorage, LocalStorage, S3Storage, SyncStorage}};

#[derive(Debug)]
struct AttributeChange {
    change_id: String,
    entity_type: String,
    entity_id: String,
    attribute: String,
    new_value: serde_json::Value,
}

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

    /// # Sync Algorithm
    /// 
    /// The goal is for every device/replica/author to have a complete copy of
    /// the changelog. From the changelog we can replicate the entity state
    /// at any point in time from the perspective of any author.
    /// 
    /// 1. Get the sets of local and remote change_ids.
    /// 2. For any remote change_id not in the local set, download and insert
    /// it, setting merged = false. 
    /// 3. For any local change_id not in the remote set, upload it.
    /// 4. Get the local changes that are marked unmerged.
    /// 5. Map these, parsing the JSON, to &[(change_id, entity_name, entity_id, attribute, new_value)]
    /// 6. Reduce this to only the newest change for each (entity_name, entity_id, attribute)
    /// 7. Group these by (entity_name, entity_id)
    /// 8. For each group:
    ///     1. Read the entity
    ///     2. Update the entity fields from the values in the group
    ///     3. Save the entity
    pub fn sync(&self, db: &Db) -> Result<()> {
        // 1. Get the sets of local and remote change_ids.
        let local_change_ids = self.list_local_change_ids(db)?
            .into_iter().collect::<HashSet<_>>();
        let remote_change_ids = self.list_remote_change_ids()?
            .into_iter().collect::<HashSet<_>>();

        // 2. For any remote change_id not in the local set, download and insert
        // it, setting merged = false. 
        let missing_remote_change_ids = remote_change_ids.iter()
            .filter(|id| !local_change_ids.contains(*id))
            .collect::<Vec<_>>();
        for remote_change_id in missing_remote_change_ids {
            let mut change = self.get_remote_change(remote_change_id)?;
            change.merged = false;
            self.put_local_change(db, &change)?;
        }

        // 3. For any local change_id not in the remote set, upload it.
        let missing_local_change_ids = local_change_ids.iter()
            .filter(|id| !remote_change_ids.contains(*id))
            .collect::<Vec<_>>();
        for local_change_id in missing_local_change_ids {
            let change = self.get_local_change(db, local_change_id)?;
            self.put_remote_change(&change)?;
        }

        // 4-8. Process unmerged changes
        self.merge_unmerged_changes(db)
    }

    fn merge_unmerged_changes(&self, db: &Db) -> Result<()> {
        db.transaction(|txn| {
            // Get unmerged changes
            // Vec<ChangeRecord>
            let unmerged_changes = txn.query::<ChangeRecord, _>(
                "SELECT id, author_id, entity_type, entity_id, old_values, new_values, merged 
                 FROM ZV_CHANGE 
                 WHERE merged = false 
                 ORDER BY id",
                ()
            )?;

            // Extract individual attribute changes
            // Vec<AttributeChange>
            let attribute_changes = self.extract_attribute_changes(&unmerged_changes)?;

            // Reduce to newest changes per attribute
            // HashMap<(entity_type, entity_id, attribute), AttributeChange>
            let newest_changes = self.reduce_to_newest_changes(attribute_changes);

            // Group by entity and apply updates
            // HashMap<(entity_type, entity_id), Vec<AttributeChange>>
            let entity_updates = self.group_changes_by_entity(newest_changes);

            // Apply all entity updates
            for ((entity_type, entity_id), changes) in entity_updates {
                self.apply_entity_updates(txn, &entity_type, &entity_id, changes)?;
            }

            // Mark all changes as merged
            txn.txn().execute(
                "UPDATE ZV_CHANGE SET merged = true WHERE merged = false",
                []
            )?;

            Ok(())
        })
    }

    fn extract_attribute_changes(&self, unmerged_changes: &[ChangeRecord]) -> Result<Vec<AttributeChange>> {
        let mut attribute_changes = Vec::new();

        for change in unmerged_changes {
            if let Some(new_values_json) = &change.new_values {
                if let Ok(new_values) = serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(new_values_json) {
                    for (attribute, value) in new_values {
                        attribute_changes.push(AttributeChange {
                            change_id: change.id.clone(),
                            entity_type: change.entity_type.clone(),
                            entity_id: change.entity_id.clone(),
                            attribute,
                            new_value: value,
                        });
                    }
                }
            }
        }

        Ok(attribute_changes)
    }

    fn reduce_to_newest_changes(&self, attribute_changes: Vec<AttributeChange>) -> HashMap<(String, String, String), AttributeChange> {
        let mut newest_changes: HashMap<(String, String, String), AttributeChange> = HashMap::new();

        for change in attribute_changes {
            let key = (
                change.entity_type.clone(), 
                change.entity_id.clone(), 
                change.attribute.clone()
            );

            match newest_changes.get(&key) {
                Some(existing) if existing.change_id >= change.change_id => {
                    // Keep existing (it's newer)
                }
                _ => {
                    // Insert new or replace with newer
                    newest_changes.insert(key, change);
                }
            }
        }

        newest_changes
    }

    fn group_changes_by_entity(&self, newest_changes: HashMap<(String, String, String), AttributeChange>) -> HashMap<(String, String), Vec<AttributeChange>> {
        let mut entity_updates = HashMap::new();

        for (_, change) in newest_changes {
            let key = (change.entity_type.clone(), change.entity_id.clone());
            entity_updates.entry(key).or_insert_with(Vec::new).push(change);
        }

        entity_updates
    }

    fn apply_entity_updates(&self, txn: &DbTransaction, entity_type: &str, entity_id: &str, changes: Vec<AttributeChange>) -> Result<()> {
        // Start with entity ID
        let mut entity_json = serde_json::Map::new();
        entity_json.insert("id".to_string(), serde_json::Value::String(entity_id.to_string()));

        // Read existing entity if it exists
        if self.entity_exists(txn, entity_type, entity_id)? {
            self.read_existing_entity(txn, entity_type, entity_id, &mut entity_json)?;
        }

        // Apply all attribute changes
        for change in changes {
            entity_json.insert(change.attribute, change.new_value);
        }

        // Save the updated entity
        txn.save_dynamic(entity_type, &entity_json)?;

        Ok(())
    }

    fn entity_exists(&self, txn: &DbTransaction, entity_type: &str, entity_id: &str) -> Result<bool> {
        Ok(txn.txn().query_row(
            &format!("SELECT 1 FROM {} WHERE id = ?", entity_type),
            rusqlite::params![entity_id],
            |_| Ok(())
        ).is_ok())
    }

    fn read_existing_entity(&self, txn: &DbTransaction, entity_type: &str, entity_id: &str, entity_json: &mut serde_json::Map<String, serde_json::Value>) -> Result<()> {
        let query = format!("SELECT * FROM {} WHERE id = ?", entity_type);
        let mut stmt = txn.txn().prepare(&query)?;
        
        let column_names: Vec<String> = stmt.column_names()
            .into_iter()
            .map(|s| s.to_string())
            .collect();

        // TODO smells
        stmt.query_row(rusqlite::params![entity_id], |row| {
            for (idx, column_name) in column_names.iter().enumerate() {
                if column_name != "id" {
                    // Try to get value as different types
                    if let Ok(val) = row.get::<_, Option<String>>(idx) {
                        entity_json.insert(
                            column_name.clone(),
                            val.map(serde_json::Value::String).unwrap_or(serde_json::Value::Null)
                        );
                    } else if let Ok(val) = row.get::<_, Option<i64>>(idx) {
                        entity_json.insert(
                            column_name.clone(),
                            val.map(|v| serde_json::Value::Number(v.into())).unwrap_or(serde_json::Value::Null)
                        );
                    } else if let Ok(val) = row.get::<_, Option<f64>>(idx) {
                        entity_json.insert(
                            column_name.clone(),
                            val.and_then(|v| serde_json::Number::from_f64(v))
                                .map(serde_json::Value::Number)
                                .unwrap_or(serde_json::Value::Null)
                        );
                    } else if let Ok(val) = row.get::<_, Option<bool>>(idx) {
                        entity_json.insert(
                            column_name.clone(),
                            val.map(serde_json::Value::Bool).unwrap_or(serde_json::Value::Null)
                        );
                    }
                }
            }
            Ok(())
        })?;

        Ok(())
    }

    fn list_local_change_ids(&self, db: &Db) -> Result<Vec<String>> {
        Ok(db.query::<ChangeRecord, _>("SELECT * FROM ZV_CHANGE ORDER BY id ASC", ())?
            .iter().map(|change| change.id.clone())
            .collect())
    }

    fn list_remote_change_ids(&self) -> Result<Vec<String>> {
        let prefix = "changes/";
        let files = self.storage.list(prefix)?;
        
        let mut change_ids = Vec::new();
        for file in files {
            if let Some(path) = file.strip_prefix(prefix) {
                if let Some(change_id) = path.strip_suffix(".json") {
                    change_ids.push(change_id.to_string());
                }
            }
        }
        
        change_ids.sort();
        Ok(change_ids)
    }

    fn get_local_change(&self, db: &Db, change_id: &str) -> Result<ChangeRecord> {
        db.transaction(|txn| {
            let mut stmt = txn.txn().prepare(
                "SELECT id, author_id, entity_type, entity_id, old_values, new_values, merged
                 FROM ZV_CHANGE 
                 WHERE id = ?"
            )?;
            
            let change = stmt.query_row(rusqlite::params![change_id], |row| {
                Ok(ChangeRecord {
                    id: row.get(0)?,
                    author_id: row.get(1)?,
                    entity_type: row.get(2)?,
                    entity_id: row.get(3)?,
                    old_values: row.get(4)?,
                    new_values: row.get(5)?,
                    merged: row.get(6)?,
                })
            })?;
            
            Ok(change)
        })
    }

    fn get_remote_change(&self, change_id: &str) -> Result<ChangeRecord> {
        let path = format!("changes/{}.json", change_id);
        let data = self.storage.get(&path)?;
        let change: ChangeRecord = serde_json::from_slice(&data)?;
        Ok(change)
    }

    fn put_local_change(&self, db: &Db, change: &ChangeRecord) -> Result<()> {
        db.transaction(|txn| {
            txn.txn().execute(
                "INSERT OR IGNORE INTO ZV_CHANGE (id, author_id, entity_type, entity_id, old_values, new_values, merged) 
                 VALUES (?, ?, ?, ?, ?, ?, false)",
                rusqlite::params![
                    &change.id,
                    &change.author_id,
                    &change.entity_type,
                    &change.entity_id,
                    &change.old_values,
                    &change.new_values,
                ]
            )?;
            Ok(())
        })
    }

    fn put_remote_change(&self, change: &ChangeRecord) -> Result<()> {
        let path = format!("changes/{}.json", change.id);
        let data = serde_json::to_vec_pretty(change)?;
        self.storage.put(&path, &data)?;
        Ok(())
    }
}

#[derive(Default)]
pub struct SyncEngineBuilder {
    storage: Option<Box<dyn SyncStorage>>,
    passphrase: Option<String>,
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

    pub fn build(self) -> Result<SyncEngine> {
        if let Some(passphrase) = self.passphrase {
            let storage = EncryptedStorage::new(self.storage.unwrap(), passphrase);
            SyncEngine::new_with_storage(Box::new(storage))
        }
        else {
            SyncEngine::new_with_storage(self.storage.unwrap())
        }
    }
}

#[cfg(test)]
mod tests {
    use rusqlite_migration::{Migrations, M};
    use serde::{Deserialize, Serialize};
    use crate::{Db, sync::SyncEngine};

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
}