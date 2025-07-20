use anyhow::Result;
use uuid::Uuid;
use rusqlite::{params, OptionalExtension};

use crate::{db::Db, sync::{Change, Changes, EncryptedStorage, InMemoryStorage, LocalStorage, ReplicaMetadata, S3Storage, SyncStorage}};

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
        // TODO: Handle large change sets more efficiently (pagination, streaming)
        self.pull(db)?;
        self.push(db)?;
        Ok(())
    }

    fn pull(&self, db: &Db) -> Result<()> {
        let my_replica_id = db.get_database_uuid()?;
        
        let replicas = self.get_replicas()?;
        
        for replica in replicas {
            if replica.replica_id == my_replica_id {
                continue;
            }
            
            self.pull_replica_changes(db, &replica)?;
        }
        Ok(())
    }
    
    fn pull_replica_changes(&self, db: &Db, replica: &ReplicaMetadata) -> Result<()> {
        let latest_change_id = self.get_latest_change_for_replica(db, &replica.replica_id)?;
        
        let change_files = self.list_change_files(&replica.replica_id)?;
        
        for change_file in change_files {
            if let Some(ref latest_id) = latest_change_id {
                if change_file <= *latest_id {
                    continue;
                }
            }
            
            let bundle = self.download_change_file(&replica.replica_id, &change_file)?;
            self.apply_changes(db, bundle)?;
        }
        
        Ok(())
    }
    
    fn get_latest_change_for_replica(&self, db: &Db, replica_id: &str) -> Result<Option<String>> {
        db.transaction(|txn| {
            let mut stmt = txn.txn.prepare(
                "SELECT id FROM ZV_CHANGE 
                 WHERE author_id = ? 
                 ORDER BY id DESC 
                 LIMIT 1"
            )?;
            
            let result = stmt.query_row(params![replica_id], |row| {
                row.get::<_, String>(0)
            }).optional()?;
            
            Ok(result)
        })
    }
    
    fn list_change_files(&self, replica_id: &str) -> Result<Vec<String>> {
        let prefix = format!("changes/{}/", replica_id);
        let files = self.storage.list(&prefix)?;
        let mut change_files: Vec<String> = files.iter()
            .filter_map(|f| {
                f.strip_prefix(&prefix)
                    .and_then(|s| s.strip_suffix(".json"))
                    .map(|s| s.to_string())
            })
            .collect();
        change_files.sort();
        Ok(change_files)
    }
    
    fn download_change_file(&self, replica_id: &str, change_file: &str) -> Result<Changes> {
        let path = format!("changes/{}/{}.json", replica_id, change_file);
        let data = self.storage.get(&path)?;
        let bundle: Changes = serde_json::from_slice(&data)?;
        Ok(bundle)
    }
    
    fn apply_changes(&self, db: &Db, bundle: Changes) -> Result<()> {
        db.transaction(|txn| {
            for change in bundle.changes {
                // First insert the change into ZV_CHANGE
                txn.txn.execute(
                    "INSERT OR IGNORE INTO ZV_CHANGE (id, author_id, entity_type, entity_id, old_values, new_values) 
                     VALUES (?, ?, ?, ?, ?, ?)",
                    params![
                        &change.id,
                        &change.author_id,
                        &change.entity_type,
                        &change.entity_id,
                        &change.old_values,
                        &change.new_values,
                    ]
                )?;
                
                // Always apply changes, but only apply winning attributes
                self.apply_change_to_entity(txn, &change)?;
            }
            
            Ok(())
        })
    }
    
    
    fn apply_change_to_entity(&self, txn: &crate::db::transaction::DbTransaction, change: &Change) -> Result<()> {
        // Parse the new_values to see which attributes this change wants to update
        let new_values_to_apply = if let Some(ref new_values_json) = change.new_values {
            let new_values: serde_json::Map<String, serde_json::Value> = serde_json::from_str(new_values_json)?;
            let mut winning_attributes = serde_json::Map::new();
            
            // For each attribute in this change, check if it's the latest
            for (attribute, value) in new_values {
                if self.is_latest_change_for_attribute(txn, &change.entity_type, &change.entity_id, &attribute, &change.id)? {
                    winning_attributes.insert(attribute, value);
                }
            }
            
            winning_attributes
        } else {
            serde_json::Map::new()
        };
        
        // If no attributes won the LWW check, nothing to apply
        if new_values_to_apply.is_empty() {
            return Ok(());
        }
        
        // Get the current entity state from the database if it exists
        let conn = txn.txn;
        let check_sql = format!("SELECT * FROM {} WHERE id = ?", change.entity_type);
        
        let mut stmt = conn.prepare(&check_sql)?;
        let existing_entity = stmt.query_row(params![&change.entity_id], |row| {
            // Convert the row to a JSON object
            let column_count = row.as_ref().column_count();
            let mut entity_map = serde_json::Map::new();
            
            for i in 0..column_count {
                let column_name = row.as_ref().column_name(i)?;
                // Try to get the value as different types
                if let Ok(val) = row.get::<_, Option<String>>(i) {
                    entity_map.insert(
                        column_name.to_string(),
                        val.map(serde_json::Value::String).unwrap_or(serde_json::Value::Null)
                    );
                } else if let Ok(val) = row.get::<_, Option<i64>>(i) {
                    entity_map.insert(
                        column_name.to_string(),
                        val.map(|v| serde_json::Value::Number(v.into())).unwrap_or(serde_json::Value::Null)
                    );
                } else if let Ok(val) = row.get::<_, Option<f64>>(i) {
                    entity_map.insert(
                        column_name.to_string(),
                        val.and_then(|v| serde_json::Number::from_f64(v))
                            .map(serde_json::Value::Number)
                            .unwrap_or(serde_json::Value::Null)
                    );
                }
            }
            Ok(entity_map)
        }).optional()?;
        
        // Build the final entity state
        let entity_data = if let Some(mut existing) = existing_entity {
            // Update existing entity with only the winning attributes
            for (key, value) in new_values_to_apply {
                existing.insert(key, value);
            }
            existing
        } else {
            // Create new entity - need to build from old_values + winning new_values
            let mut entity_map = serde_json::Map::new();
            
            // Always include the ID
            entity_map.insert("id".to_string(), serde_json::Value::String(change.entity_id.clone()));
            
            // First apply old values if they exist
            if let Some(old_json) = &change.old_values {
                let old_map: serde_json::Map<String, serde_json::Value> = serde_json::from_str(old_json)?;
                for (k, v) in old_map {
                    entity_map.insert(k, v);
                }
            }
            
            // Then apply only the winning new values
            for (k, v) in new_values_to_apply {
                entity_map.insert(k, v);
            }
            
            entity_map
        };
        
        // Use save_dynamic to apply the entity
        txn.save_dynamic(&change.entity_type, &entity_data)?;
        
        Ok(())
    }
    
    /// Check if a change is the latest for a given attribute
    fn is_latest_change_for_attribute(&self, txn: &crate::db::transaction::DbTransaction, 
                                      entity_type: &str, entity_id: &str, 
                                      attribute: &str, change_id: &str) -> Result<bool> {
        let mut stmt = txn.txn.prepare(
            "SELECT id FROM ZV_CHANGE 
             WHERE entity_type = ? AND entity_id = ? 
             AND (json_type(old_values, ?) IS NOT NULL OR json_type(new_values, ?) IS NOT NULL)
             ORDER BY id DESC 
             LIMIT 1"
        )?;
        
        let json_path = format!("$.{}", attribute);
        let latest_id: String = stmt.query_row(
            params![entity_type, entity_id, &json_path, &json_path],
            |row| row.get(0)
        ).unwrap_or_else(|_| change_id.to_string());
        
        Ok(latest_id <= change_id.to_string())
    }

    fn push(&self, db: &Db) -> Result<()> {
        let my_replica_id = db.get_database_uuid()?;
        
        let latest_pushed_change = self.get_latest_pushed_change(&my_replica_id)?;
        
        let unpushed_changes = self.get_unpushed_changes(db, &my_replica_id, latest_pushed_change.as_deref())?;
        
        if unpushed_changes.is_empty() {
            return Ok(());
        }
        
        let change_file_id = Uuid::now_v7().to_string();
        let bundle = Changes {
            replica_id: my_replica_id.clone(),
            changes: unpushed_changes,
        };
        
        let _first_change_id = bundle.changes.first().map(|c| c.id.clone()).unwrap();
        let last_change_id = bundle.changes.last().map(|c| c.id.clone()).unwrap();
        
        let data = serde_json::to_vec_pretty(&bundle)?;
        let change_path = format!("changes/{}/{}.json", my_replica_id, change_file_id);
        self.storage.put(&change_path, &data)?;
        
        let replica_info = ReplicaMetadata {
            replica_id: my_replica_id.clone(),
            latest_change_id: last_change_id,
            latest_change_file: change_file_id,
        };
        let replica_data = serde_json::to_vec_pretty(&replica_info)?;
        self.storage.put(&format!("replicas/{}.json", my_replica_id), &replica_data)?;
        
        Ok(())
    }
    
    fn get_latest_pushed_change(&self, replica_id: &str) -> Result<Option<String>> {
        match self.get_replica_metadata(replica_id) {
            Ok(info) => Ok(Some(info.latest_change_id)),
            Err(_) => Ok(None),
        }
    }
    
    fn get_unpushed_changes(&self, db: &Db, replica_id: &str, after_change_id: Option<&str>) -> Result<Vec<Change>> {
        db.transaction(|txn| {
            let conn = txn.txn;
            
            // Use "0" as minimum value when no after_change_id is provided
            // since UUIDv7s are lexicographically sortable and will all be > "0"
            let after_id = after_change_id.unwrap_or("0");
            
            let mut stmt = conn.prepare(
                "SELECT id, author_id, entity_type, entity_id, old_values, new_values 
                 FROM ZV_CHANGE 
                 WHERE author_id = ? AND id > ? 
                 ORDER BY id"
            )?;
            
            let mut changes = Vec::new();
            let rows = stmt.query_map(params![replica_id, after_id], |row| {
                Ok(Change {
                    id: row.get(0)?,
                    author_id: row.get(1)?,
                    entity_type: row.get(2)?,
                    entity_id: row.get(3)?,
                    old_values: row.get(4)?,
                    new_values: row.get(5)?,
                })
            })?;
            
            for row in rows {
                changes.push(row?);
            }
            
            Ok(changes)
        })
    }

    fn get_replicas(&self) -> Result<Vec<ReplicaMetadata>> {
        let replicas = self.list_replica_ids()?.iter()
            // TODO log or mark the ones that error
            .filter_map(|replica_id| self.get_replica_metadata(replica_id).ok())
            .collect::<Vec<_>>();
        Ok(replicas)
    }

    fn list_replica_ids(&self) -> Result<Vec<String>> {
        let files = self.storage.list("replicas/")?;
        let replica_ids = files.iter()
            .filter_map(|r| {
                r.strip_prefix("replicas/")
                    .and_then(|s| s.strip_suffix(".json"))
                    .map(|s| s.to_string())
            })
            .collect::<Vec<_>>();
        Ok(replica_ids)
    }

    fn get_replica_metadata(&self, replica_id: &str) -> Result<ReplicaMetadata> {
        let data = self.storage.get(&format!("replicas/{}.json", replica_id))?;
        Ok(serde_json::from_slice(&data)?)
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

    #[derive(Serialize, Deserialize, Clone, Debug, Default)]
    struct Artist {
        pub id: String,
        pub name: String,
        pub country: Option<String>,
    }

    #[derive(Serialize, Deserialize, Clone, Debug, Default)]
    struct Pet {
        pub id: String,
        pub name: String,
        pub description: Option<String>,
        pub rating: Option<i32>,
        pub snack: Option<String>,
    }

    #[test]
    fn per_attribute_lww_resolution() -> anyhow::Result<()> {
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Pet (id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, description TEXT, rating INTEGER, snack TEXT);"),
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

        // User A inserts Pet
        let pet_id = "pet-1".to_string();
        db1.save(&Pet {
            id: pet_id.clone(),
            name: "Dog".to_string(),
            description: Some("Bestie".to_string()),
            rating: None,
            snack: None,
        })?;

        // Sync to get all replicas in sync
        sync_engine.sync(&db1)?;
        sync_engine.sync(&db2)?;
        sync_engine.sync(&db3)?;
        sync_engine.sync(&db1)?;
        sync_engine.sync(&db2)?;

        // Simulate different users making changes to different attributes
        // User B (db2) changes rating
        db2.save(&Pet {
            id: pet_id.clone(),
            name: "Dog".to_string(),
            description: Some("Bestie".to_string()),
            rating: Some(12),
            snack: None,
        })?;

        // User C (db3) changes snack  
        db3.save(&Pet {
            id: pet_id.clone(),
            name: "Dog".to_string(),
            description: Some("Bestie".to_string()),
            rating: None,
            snack: Some("Biscuit".to_string()),
        })?;

        // User B (db2) changes snack again (should win over User C due to later timestamp)
        std::thread::sleep(std::time::Duration::from_millis(1)); // Ensure different timestamp
        db2.save(&Pet {
            id: pet_id.clone(),
            name: "Dog".to_string(),
            description: Some("Bestie".to_string()),
            rating: Some(12),
            snack: Some("Bone".to_string()),
        })?;

        // User C (db3) changes rating (should win over User B's first rating change)
        std::thread::sleep(std::time::Duration::from_millis(1)); // Ensure different timestamp
        db3.save(&Pet {
            id: pet_id.clone(),
            name: "Dog".to_string(),
            description: Some("Bestie".to_string()),
            rating: Some(13),
            snack: Some("Biscuit".to_string()),
        })?;

        // Sync everything
        sync_engine.sync(&db1)?;
        sync_engine.sync(&db2)?;
        sync_engine.sync(&db3)?;
        sync_engine.sync(&db1)?;
        sync_engine.sync(&db2)?;
        sync_engine.sync(&db3)?;

        // Check final state - should have per-attribute LWW
        // Expected: rating=13 (from User C's latest change), snack=Bone (from User B's latest change)
        let final_pet1: Option<Pet> = db1.get(&pet_id)?;
        let final_pet2: Option<Pet> = db2.get(&pet_id)?;
        let final_pet3: Option<Pet> = db3.get(&pet_id)?;

        assert!(final_pet1.is_some());
        assert!(final_pet2.is_some());
        assert!(final_pet3.is_some());

        let pet1 = final_pet1.unwrap();
        let pet2 = final_pet2.unwrap();
        let pet3 = final_pet3.unwrap();

        // All replicas should have identical final state
        assert_eq!(pet1.name, "Dog");
        assert_eq!(pet1.description, Some("Bestie".to_string()));
        assert_eq!(pet1.rating, Some(13)); // User C's last rating change should win
        assert_eq!(pet1.snack, Some("Bone".to_string())); // User B's last snack change should win

        assert_eq!(pet1.rating, pet2.rating);
        assert_eq!(pet1.snack, pet2.snack);
        assert_eq!(pet1.rating, pet3.rating);
        assert_eq!(pet1.snack, pet3.snack);

        Ok(())
    }
}