use std::collections::HashSet;

use anyhow::Result;

use crate::{db::{ChangeRecord, Db}, sync::{EncryptedStorage, InMemoryStorage, LocalStorage, S3Storage, SyncStorage}};

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
        // TODO I think almost everything in this function can be done in parallel

        // gets lists of local and remote change_ids into HashSets for quick
        // lookups
        let local_change_ids = self.list_local_change_ids(db)?
            .into_iter().collect::<HashSet<_>>();
        let remote_change_ids = self.list_remote_change_ids()?
            .into_iter().collect::<HashSet<_>>();

        // for any changes on the remote, but not local, download and save them
        // any newly inserted changes should be marked unmerged
        let missing_remote_change_ids = remote_change_ids.iter()
            .filter(|id| !local_change_ids.contains(*id))
            .collect::<Vec<_>>();
        for remote_change_id in missing_remote_change_ids {
            let change = self.get_remote_change(remote_change_id)?;
            self.put_local_change(db, &change)?;
        }

        // and for any changes on the local, but not the remote, upload them
        let missing_local_change_ids = local_change_ids.iter()
            .filter(|id| !remote_change_ids.contains(*id))
            .collect::<Vec<_>>();
        for local_change_id in missing_local_change_ids {
            let change = self.get_local_change(db, local_change_id)?;
            self.put_remote_change(&change)?;
        }

        // TODO read the set of unmerged (entity_type, entity_id) and rebuild
        // them.
        // This all needs to happen in a single transaction, I think.
        // So something like select * from zv_change where merged = false
        // to get the unmerged changes, and then we either merge them
        // directly or perform rebuild ops on the referenced entities.

        

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
            let mut stmt = txn.raw().prepare(
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
                    new_values: row.get(5)?
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
            txn.raw().execute(
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