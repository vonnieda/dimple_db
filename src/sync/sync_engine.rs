use anyhow::Result;
use serde::{Deserialize, Serialize};

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


    //  4. Sync Algorithm
    // 	1. Pull new remote changes
    // 		1. Get list of replicas by listing `*.dimple_db` and extracting the UUID part.
    // 		2. (Optionally) Register unknown replicas, "Want to sync with replica id XYZ?", and then save their public key.
    // 		3. For each replica:
    // 			1. Read LATEST.json to get the id of the newest change.
    // 			2. See what the latest change stored for this replica is: `SELECT * FROM ZV_CHANGE WHERE author_id = {} ORDER BY id DESC LIMIT 1`
    // 			3. If there are newer changes, download the new files in order
    // 			4. For each new change
    // 				1. Insert the change into the ZV_CHANGE table
    // 				2. If the change is newer than the last change to the given (entity, key, attribute) then update the entity in the database as well
    // 	2. Push new local changes
    // 		1. Read LATEST.json to get the id of the latest change uploaded
    // 		2. If there are any newer changes stored:
    // 			1. Upload a new Sqlite database containing just the new changes in ZV_CHANGE
    // 			2. Update LATEST.json with the first and last change ids from the file just uploaded.
    // NOTE: Don't do any storage IO in a transaction
    pub fn sync(&self, db: &Db) -> Result<()> {
        self.pull()?;
        self.push()?;
        Ok(())
    }

    fn pull(&self) -> Result<()> {
        for replica in self.list_replicas()? {
            let latest = self.get_latest(&replica)?;
        }
        Ok(())
    }

    fn push(&self) -> Result<()> {
        Ok(())
    }

    fn list_replicas(&self) -> Result<Vec<String>> {
        // TODO okay, first issue is we end up listing objects we don't need
        // cause not using a good prefix. 
        self.storage.list("")
    }

    fn get_latest(&self, replica_id: &str) -> Result<LatestChanges> {
        todo!()
    }
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct LatestChanges {
    pub author_id: String,
    pub latest_file: String,
    pub latest_file_first_change_id: String,
    pub latest_file_last_change_id: String,
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
        let sync_engine = SyncEngine::builder()
            .in_memory()
            .encrypted("correct horse battery staple")
            .build()?;
        sync_engine.sync(&db1)?;
        sync_engine.sync(&db2)?;
        assert_eq!(db1.query::<Artist, _>("SELECT * FROM Artist", ())?.len(), 1);
        assert_eq!(db2.query::<Artist, _>("SELECT * FROM Artist", ())?.len(), 1);
        Ok(())
    }

    #[derive(Serialize, Deserialize, Clone, Debug, Default)]
    struct Artist {
        pub id: String,
        pub name: String,
        pub country: Option<String>,
    }
}