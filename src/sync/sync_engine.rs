use anyhow::Result;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use rusqlite::{params, OptionalExtension};

use crate::{db::Db, sync::{encrypted_storage::EncryptedStorage, local_storage::LocalStorage, memory_storage::InMemoryStorage, s3_storage::S3Storage, SyncStorage}};

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
        // TODO note changes are potentially very large and we're just
        // loading them all into memory
        // TODO we aren't receiving notifications, or firing subscriptions, for
        // stuff updated by sync. I think we should use DbTransaction.save_untracked
        // if possible so there is only one place where this all happens.
        // TODO we can't handle NOT NULL when creating a new object from sync
        // it might be best to just use old_values and new_values, instead of
        // singular. 
        // So, this is why we had ZV_TRANSACTION in the first place and it occurs
        // to me now that they were just representing different things, which
        // was fine. A DbTransaction is a set, really, of ZV_TRANSACTION
        // but it would probably be best to just change Zv_TRANSACTION and
        // ZV_CHANGE to like ZV_CHANGE and ZV_CHANGE_COLUMN or just put a
        // blob containing all the changed columns in ZV_CHANGE.
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
            let conn = txn.connection();
            let mut stmt = conn.prepare(
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
            let conn = txn.connection();
            
            for change in bundle.changes {
                conn.execute(
                    "INSERT OR IGNORE INTO ZV_CHANGE (id, author_id, entity_type, entity_id, attribute, old_value, new_value) 
                     VALUES (?, ?, ?, ?, ?, ?, ?)",
                    params![
                        &change.id,
                        &change.author_id,
                        &change.entity_type,
                        &change.entity_id,
                        &change.attribute,
                        &change.old_value,
                        &change.new_value,
                    ]
                )?;
                
                if self.is_latest_change_for_attribute(conn, &change)? {
                    self.apply_change_to_entity(conn, &change)?;
                }
            }
            
            Ok(())
        })
    }
    
    fn is_latest_change_for_attribute(&self, conn: &rusqlite::Connection, change: &Change) -> Result<bool> {
        let mut stmt = conn.prepare(
            "SELECT id FROM ZV_CHANGE 
             WHERE entity_type = ? AND entity_id = ? AND attribute = ? 
             ORDER BY id DESC 
             LIMIT 1"
        )?;
        
        let latest_id: String = stmt.query_row(
            params![&change.entity_type, &change.entity_id, &change.attribute],
            |row| row.get(0)
        ).unwrap_or_else(|_| change.id.clone());
        
        Ok(latest_id <= change.id)
    }
    
    fn apply_change_to_entity(&self, conn: &rusqlite::Connection, change: &Change) -> Result<()> {
        // Parse the JSON value to get the actual value
        let parsed_value = if let Some(ref new_value) = change.new_value {
            // The value is stored as JSON, so we need to parse it
            let parsed: serde_json::Value = serde_json::from_str(new_value)?;
            match parsed {
                serde_json::Value::Null => None,
                serde_json::Value::String(s) => Some(s),
                _ => Some(parsed.to_string()),
            }
        } else {
            None
        };
        
        // First check if the entity exists
        let check_sql = format!("SELECT COUNT(*) FROM {} WHERE id = ?", change.entity_type);
        let count: i64 = conn.query_row(&check_sql, params![&change.entity_id], |row| row.get(0))?;
        
        if count == 0 {
            // Entity doesn't exist, we need to insert it
            let insert_sql = format!(
                "INSERT INTO {} (id, {}) VALUES (?, ?)",
                change.entity_type, change.attribute
            );
            conn.execute(&insert_sql, params![&change.entity_id, &parsed_value])?;
        } else if parsed_value.is_some() {
            // Entity exists, update it
            let sql = format!(
                "UPDATE {} SET {} = ? WHERE id = ?",
                change.entity_type, change.attribute
            );
            conn.execute(&sql, params![&parsed_value, &change.entity_id])?;
        }
        Ok(())
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
        
        let data = serde_json::to_vec(&bundle)?;
        let change_path = format!("changes/{}/{}.json", my_replica_id, change_file_id);
        self.storage.put(&change_path, &data)?;
        
        let replica_info = ReplicaMetadata {
            replica_id: my_replica_id.clone(),
            latest_change_id: last_change_id,
            latest_change_file: change_file_id,
        };
        let replica_data = serde_json::to_vec(&replica_info)?;
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
            let conn = txn.connection();
            
            // Use "0" as minimum value when no after_change_id is provided
            // since UUIDv7s are lexicographically sortable and will all be > "0"
            let after_id = after_change_id.unwrap_or("0");
            
            let mut stmt = conn.prepare(
                "SELECT id, author_id, entity_type, entity_id, attribute, old_value, new_value 
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
                    attribute: row.get(4)?,
                    old_value: row.get(5)?,
                    new_value: row.get(6)?,
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

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct ReplicaMetadata {
    pub replica_id: String,
    pub latest_change_id: String,
    pub latest_change_file: String,
}

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct Changes {
    pub replica_id: String,
    pub changes: Vec<Change>,
}

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct Change {
    pub id: String,
    pub author_id: String,
    pub entity_type: String,
    pub entity_id: String,
    pub attribute: String,
    pub old_value: Option<String>,
    pub new_value: Option<String>,
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

mod tests {
    use anyhow::Result;
    use rusqlite_migration::{Migrations, M};
    use serde::{Deserialize, Serialize};
    use crate::{sync::SyncEngine, Db};

    #[test]
    fn basic_sync() -> Result<()> {
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
}