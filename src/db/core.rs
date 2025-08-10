use std::{sync::{mpsc::{self, Receiver, Sender}, Arc, Mutex}};

use anyhow::Result;
use r2d2::{CustomizeConnection, Pool};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{functions::FunctionFlags, Params, Transaction};
use rusqlite_migration::{Migrations};
use uuid::Uuid;

use crate::db::{query::QuerySubscription, transaction::DbTransaction, DbEvent, Entity};

#[derive(Clone)]
pub struct Db {
    pool: Pool<SqliteConnectionManager>,
    subscribers: Arc<Mutex<Vec<Sender<DbEvent>>>>,
    database_uuid: String,
}

impl Db {
    pub fn open_memory() -> Result<Self> {
        let manager = r2d2_sqlite::SqliteConnectionManager::memory();
        let pool = r2d2::Pool::builder()
            .connection_customizer(Box::new(DbConnectionCustomizer{}))
            // https://beets.io/blog/sqlite-nightmare.html
            // https://sqlite.org/wal.html
            // > 9. Sometimes Queries Return SQLITE_BUSY In WAL Mode
            .max_size(1)
            .build(manager)?;
        Self::from_pool(pool)
    }

    pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let manager = r2d2_sqlite::SqliteConnectionManager::file(path);
        let pool = r2d2::Pool::builder()
            .connection_customizer(Box::new(DbConnectionCustomizer{}))
            // https://beets.io/blog/sqlite-nightmare.html
            .max_size(1)
            .build(manager)?;
        Self::from_pool(pool)
    }

    pub fn migrate(&self, migrations: &Migrations) -> Result<()> {
        let mut conn = self.pool.get()?;

        migrations.to_latest(&mut conn)?;

        Ok(())
    }

    /// Subscribe to be notified of any insert, update, or delete to the database.
    /// Dropped Receivers will be lazily cleaned up on the next event broadcast.
    pub fn subscribe(&self) -> Receiver<DbEvent> {
        let (tx, rx) = mpsc::channel();
        
        // Add to subscriber list
        if let Ok(mut subscribers) = self.subscribers.lock() {
            subscribers.push(tx);
        }
        
        rx
    }

    /// Calls the supplied closure with a database transaction that can be
    /// used to perform writes to the database. Commits automatically
    /// if the closure returns Ok, otherwise rolls back.
    pub fn transaction<F, R>(&self, f: F) -> Result<R>
        where F: FnOnce(&DbTransaction) -> Result<R> {
        let mut conn = self.pool.get()?;

        let mut txn = conn.transaction()?;
        txn.set_drop_behavior(rusqlite::DropBehavior::Rollback);
        let db_txn = DbTransaction::new(self, &txn);
        let result = f(&db_txn);
        if result.is_ok() {
            // Collect events before committing
            let pending_events = db_txn.take_pending_events();
            txn.commit()?;
            // Notify subscribers only after successful commit
            for event in pending_events {
                self.notify_subscribers(event);
            }
        }
        else {
            txn.rollback()?;
        }
        result
    }

    /// Shortcut to create a transaction and save a single entity.
    /// See DbTransaction.save()
    pub fn save<T: Entity>(&self, entity: &T) -> Result<T> {
        self.transaction(|t| t.save(entity))
    }

    /// Simple query without creating a transaction.
    pub fn query<E: Entity, P: Params>(&self, sql: &str, params: P) -> Result<Vec<E>> {
        let conn = self.pool.get()?;
        let mut stmt = conn.prepare(sql)?;
        let entities = serde_rusqlite::from_rows::<E>(stmt.query(params)?)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(entities)
    }

    /// Get a single entity by id without creating a transaction.
    pub fn get<E: Entity>(&self, id: &str) -> Result<Option<E>> {
        let table_name = self.table_name_for_type::<E>()?;
        let sql = format!("SELECT * FROM {} WHERE id = ? LIMIT 1", table_name);
        Ok(self.query::<E, _>(&sql, [id])?.into_iter().next())
    }

    pub fn find<T: Entity, P: Params>(&self, sql: &str, params: P) -> Result<Option<T>> {
        Ok(self.query(sql, params)?.into_iter().next())
    }

    /// Get the database's unique UUIDv7. This is created when the database is
    /// first initialized and never changes.
    pub fn get_database_uuid(&self) -> Result<String> {
        Ok(self.database_uuid.clone())
    }

    /// Performs the given query, calling the closure with the results
    /// immediately and then again any time any table referenced in the query
    /// changes. Returns a QuerySubscription that automatically unsubscribes the
    /// query on drop or via QuerySubscription.unsubscribe(). Each
    /// subscription creates a monitoring thread that uses read-only queries.
    pub fn query_subscribe<E, P, F>(&self, sql: &str, params: P, f: F) 
        -> Result<QuerySubscription> 
        where 
            E: Entity + 'static, 
            P: Params + Clone + Send + 'static, 
            F: FnMut(Vec<E>) + Send + 'static {
        QuerySubscription::new(self, sql, params, f)
    } 

    fn from_pool(pool: Pool<SqliteConnectionManager>) -> Result<Self> {
        let conn = pool.get()?;
        crate::changelog::init_change_tracking_tables(&conn)?;
        let database_uuid: String = conn.query_row(
            "SELECT value FROM ZV_METADATA WHERE key = 'database_uuid'",
            [],
            |row| row.get(0)
        )?;

        let db = Db {
            pool,
            subscribers: Arc::new(Mutex::new(Vec::new())),
            database_uuid,
        };

        Ok(db)
    }

    pub fn table_name_for_type<T>(&self) -> Result<String> {
        let full_name = std::any::type_name::<T>();
        // Extract just the struct name from the full path
        Ok(full_name.split("::").last().unwrap_or(full_name).to_string())
    }

    pub(crate) fn table_column_names(&self, txn: &Transaction, table_name: &str) -> Result<Vec<String>> {
        let mut stmt = txn.prepare(&format!("PRAGMA table_info({})", table_name))?;
        let column_names = stmt.query_map([], |row| {
            row.get::<_, String>(1) // Column name is at index 1
        })?
        .collect::<Result<Vec<_>, _>>()?;
        
        if column_names.is_empty() {
            return Err(anyhow::anyhow!("Table '{}' not found or has no columns", table_name));
        }
        
        Ok(column_names)
    }
    
    pub(crate) fn notify_subscribers(&self, event: DbEvent) {
        if let Ok(mut subscribers) = self.subscribers.lock() {
            // Send to all subscribers, remove ones that fail
            subscribers.retain(|tx| {
                tx.send(event.clone()).is_ok()
            });
        }
    }
}


#[derive(Debug)]
struct DbConnectionCustomizer;
impl CustomizeConnection<rusqlite::Connection, rusqlite::Error> for DbConnectionCustomizer {
    fn on_acquire(&self, conn: &mut rusqlite::Connection) -> Result<(), rusqlite::Error> {
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.create_scalar_function("uuid7", 0, FunctionFlags::SQLITE_UTF8, |_ctx| {
            Ok(Uuid::now_v7().to_string())
        })?;
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use anyhow::Result;
    use serde::{Deserialize, Serialize};
    use rusqlite_migration::{Migrations, M};
    use std::sync::mpsc::channel;
    use std::thread;
    use std::time::Duration;

    use crate::db::{Db, DbEvent};

    fn setup_db() -> Result<Db> {
        let db = Db::open_memory()?;
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL, summary TEXT);"),
        ]);
        db.migrate(&migrations)?;
        Ok(db)
    }

    // Database Operations
    #[test]
    fn can_create_database() -> Result<()> {
        let _ = Db::open_memory()?;
        Ok(())
    }

    #[test]
    fn type_names_map_to_table_names() -> Result<()> {
        let db = Db::open_memory()?;
        assert_eq!(db.table_name_for_type::<Artist>()?, "Artist");
        assert_eq!(db.table_name_for_type::<Album>()?, "Album");
        assert_eq!(db.table_name_for_type::<AlbumArtist>()?, "AlbumArtist");
        Ok(())
    }

    #[test]
    fn save_generates_uuid_for_new_entities() -> Result<()> {
        let db = setup_db()?;
        let artist = db.save(&Artist::default())?;
        assert!(uuid::Uuid::parse_str(&artist.id).is_ok());
        Ok(())
    }

    #[test]
    fn can_retrieve_saved_entities() -> Result<()> {
        let db = setup_db()?;
        let saved = db.save(&Artist { name: "Beatles".to_string(), ..Default::default() })?;
        let retrieved: Option<Artist> = db.get(&saved.id)?;
        
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().name, "Beatles");
        Ok(())
    }

    #[test]
    fn returns_none_for_missing_entities() -> Result<()> {
        let db = setup_db()?;
        let result: Option<Artist> = db.get("nonexistent")?;
        assert!(result.is_none());
        Ok(())
    }

    #[test]
    fn find_returns_first_matching_entity() -> Result<()> {
        let db = setup_db()?;
        
        // Save some test artists
        db.save(&Artist { name: "Beatles".to_string(), summary: Some("British rock band".to_string()), ..Default::default() })?;
        db.save(&Artist { name: "Pink Floyd".to_string(), summary: Some("Progressive rock".to_string()), ..Default::default() })?;
        db.save(&Artist { name: "Radiohead".to_string(), summary: Some("Alternative rock".to_string()), ..Default::default() })?;
        
        // Test finding by name
        let found: Option<Artist> = db.find("SELECT * FROM Artist WHERE name = ?", ["Beatles"])?;
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "Beatles");
        
        // Test finding by partial name match
        let found: Option<Artist> = db.find("SELECT * FROM Artist WHERE name LIKE ? ORDER BY name", ["P%"])?;
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "Pink Floyd"); // Should be first alphabetically
        
        // Test finding with no matches
        let found: Option<Artist> = db.find("SELECT * FROM Artist WHERE name = ?", ["Nonexistent"])?;
        assert!(found.is_none());
        
        Ok(())
    }

    #[test]
    fn find_in_transaction() -> Result<()> {
        let db = setup_db()?;
        
        db.transaction(|txn| -> Result<()> {
            // Save an artist in the transaction
            let saved = txn.save(&Artist { 
                name: "The Beatles".to_string(), 
                summary: Some("Legendary band".to_string()), 
                ..Default::default() 
            })?;
            
            // Find it using the transaction's find method
            let found: Option<Artist> = txn.find("SELECT * FROM Artist WHERE id = ?", [&saved.id])?;
            assert!(found.is_some());
            assert_eq!(found.unwrap().name, "The Beatles");
            
            // Test finding with complex query
            let found: Option<Artist> = txn.find(
                "SELECT * FROM Artist WHERE name LIKE ? AND summary IS NOT NULL", 
                ["%Beatles%"]
            )?;
            assert!(found.is_some());
            assert_eq!(found.unwrap().summary, Some("Legendary band".to_string()));
            
            Ok(())
        })?;
        
        Ok(())
    }

    

    // Schema Handling
    #[test]
    fn extra_struct_fields_ignored() -> Result<()> {
        let db = Db::open_memory()?;
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL);"), // no summary
        ]);
        db.migrate(&migrations)?;
        
        let _artist = db.save(&Artist {
            name: "Tool".to_string(),
            summary: Some("Won't be saved".to_string()),
            ..Default::default()
        })?;
        
        // Test passes if no error is thrown during save
        Ok(())
    }

    #[test]
    fn retrieved_entities_only_have_table_fields() -> Result<()> {
        let db = Db::open_memory()?;
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL);"), // no summary
        ]);
        db.migrate(&migrations)?;
        
        let saved = db.save(&Artist {
            name: "Tool".to_string(),
            summary: Some("Won't be saved".to_string()),
            ..Default::default()
        })?;
        
        assert_eq!(saved.summary, None);
        Ok(())
    }

    // Event System
    #[test]
    fn insert_triggers_insert_event() -> Result<()> {
        let db = setup_db()?;
        let receiver = db.subscribe();
        
        let artist = db.save(&Artist { name: "Radiohead".to_string(), ..Default::default() })?;
        
        let event = receiver.recv_timeout(Duration::from_millis(100))?;
        match event {
            DbEvent::Insert(table_name, entity_id) => {
                assert_eq!(table_name, "Artist");
                assert_eq!(entity_id, artist.id);
            }
            _ => panic!("Expected Insert event"),
        }
        Ok(())
    }

    #[test]
    fn update_triggers_update_event() -> Result<()> {
        let db = setup_db()?;
        let artist = db.save(&Artist { name: "Radiohead".to_string(), ..Default::default() })?;
        
        let receiver = db.subscribe();
        db.save(&Artist {
            id: artist.id.clone(),
            name: "Radiohead Updated".to_string(),
            ..Default::default()
        })?;
        
        let event = receiver.recv_timeout(Duration::from_millis(100))?;
        match event {
            DbEvent::Update(table_name, entity_id) => {
                assert_eq!(table_name, "Artist");
                assert_eq!(entity_id, artist.id);
            }
            _ => panic!("Expected Update event"),
        }
        Ok(())
    }

    #[test]
    fn query_subscribe() -> Result<()> {
        let db = setup_db()?;
        let (tx, rx) = channel::<Vec<Artist>>();
        
        let subscription = db.query_subscribe(
            "SELECT * FROM Artist", 
            (), 
            move |artists: Vec<Artist>| {
                tx.send(artists).unwrap();
            }
        )?;
        
        // Should get initial results (empty)
        let initial_results = rx.recv_timeout(Duration::from_secs(5))?;
        assert_eq!(initial_results.len(), 0);
        
        // Insert first artist
        let _artist1 = db.save(&Artist { name: "Pink Floyd".to_string(), ..Default::default() })?;
        
        // Should get updated results with 1 artist
        let results_after_first = rx.recv_timeout(Duration::from_secs(5))?;
        assert_eq!(results_after_first.len(), 1);
        assert_eq!(results_after_first[0].name, "Pink Floyd");
        
        // Insert second artist
        let _artist2 = db.save(&Artist { name: "Metallica".to_string(), ..Default::default() })?;
        
        // Should get updated results with 2 artists
        let results_after_second = rx.recv_timeout(Duration::from_secs(5))?;
        assert_eq!(results_after_second.len(), 2);

        drop(subscription);
        
        Ok(())
    }

    #[test]
    fn notifications_dont_fire_on_rollback() -> Result<()> {
        let db = setup_db()?;
        let receiver = db.subscribe();
        
        // Attempt a transaction that will fail
        let result = db.transaction(|t| -> Result<()> {
            t.save(&Artist { name: "Will Be Rolled Back".to_string(), ..Default::default() })?;
            // Force an error to trigger rollback
            anyhow::bail!("Intentional error for rollback test");
        });
        
        // Transaction should have failed
        assert!(result.is_err());
        
        // Wait a bit to ensure no delayed notifications
        thread::sleep(Duration::from_millis(100));
        
        // Should not receive any notifications since transaction rolled back
        assert!(receiver.try_recv().is_err(), "Should not receive notification on rollback");
        
        Ok(())
    }

    #[test]
    fn concurrent_database_operations_stress_test() -> Result<()> {
        // Use a temporary file database instead of memory to avoid shared cache issues
        let temp_dir = std::env::temp_dir();
        let db_path = temp_dir.join(format!("test_concurrent_{}.db", uuid::Uuid::now_v7()));
        let db = Db::open(&db_path)?;
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL, summary TEXT);"),
        ]);
        db.migrate(&migrations)?;
        let num_threads = 10;
        let operations_per_thread = 50;
        
        let mut handles = vec![];
        
        for thread_id in 0..num_threads {
            let db_clone = db.clone();
            let handle = thread::spawn(move || -> Result<()> {
                for i in 0..operations_per_thread {
                    // Mix of different operations to stress the database
                    match i % 4 {
                        0 => {
                            // Insert operation
                            let artist = Artist {
                                name: format!("Artist-{}-{}", thread_id, i),
                                summary: Some(format!("Summary for artist {} from thread {}", i, thread_id)),
                                ..Default::default()
                            };
                            db_clone.save(&artist)?;
                        },
                        1 => {
                            // Transaction with multiple operations
                            db_clone.transaction(|t| -> Result<()> {
                                let artist1 = Artist {
                                    name: format!("TxnArtist1-{}-{}", thread_id, i),
                                    ..Default::default()
                                };
                                let artist2 = Artist {
                                    name: format!("TxnArtist2-{}-{}", thread_id, i),
                                    ..Default::default()
                                };
                                t.save(&artist1)?;
                                t.save(&artist2)?;
                                Ok(())
                            })?;
                        },
                        2 => {
                            // Query operation
                            let _artists: Vec<Artist> = db_clone.query("SELECT * FROM Artist LIMIT 10", [])?;
                        },
                        3 => {
                            // Update operation (try to find and update an existing artist)
                            let artists: Vec<Artist> = db_clone.query(
                                "SELECT * FROM Artist WHERE name LIKE ? LIMIT 1", 
                                [format!("Artist-{}-%" , thread_id)]
                            )?;
                            if let Some(mut artist) = artists.into_iter().next() {
                                artist.summary = Some(format!("Updated by thread {} at op {}", thread_id, i));
                                db_clone.save(&artist)?;
                            }
                        },
                        _ => unreachable!()
                    }
                    
                    // Small delay to increase chance of contention
                    thread::sleep(Duration::from_millis(1));
                }
                Ok(())
            });
            handles.push(handle);
        }
        
        // Wait for all threads to complete and collect results
        let mut results = Vec::new();
        for handle in handles {
            match handle.join() {
                Ok(result) => results.push(result),
                Err(_) => return Err(anyhow::anyhow!("Thread panicked")),
            }
        }
        
        // Check that all threads completed successfully
        for result in results {
            result?;
        }
        
        // Verify final state - should have many artists in the database
        let final_count: Vec<Artist> = db.query("SELECT * FROM Artist", [])?;
        println!("Stress test completed. Total artists created: {}", final_count.len());
        
        // Should have at least some artists (exact count depends on timing)
        assert!(final_count.len() > 100, "Expected many artists to be created");
        
        // Cleanup temporary database file
        std::fs::remove_file(&db_path).ok();
        
        Ok(())
    }

    #[test]
    fn test_blob() -> anyhow::Result<()> {
        #[derive(Serialize, Deserialize, Default, Debug)]
        pub struct TestBlob {
            pub id: String,
            pub blob_data: Vec<u8>,
        }

        let db = Db::open_memory()?;
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE TestBlob (id TEXT NOT NULL PRIMARY KEY, blob_data BLOB NOT NULL);"),
        ]);
        db.migrate(&migrations)?;
        
        let test_blob = db.save(&TestBlob {
            blob_data: [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08].to_vec(),
            ..Default::default()
        })?;
        assert!(test_blob.blob_data.len() == 8);

        Ok(())
    }

    #[test]
    fn test_u64() -> anyhow::Result<()> {
        #[derive(Serialize, Deserialize, Default, Debug)]
        pub struct TestU64 {
            pub id: String,
            pub val: u64,
        }

        let db = Db::open_memory()?;
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE TestU64 (id TEXT NOT NULL PRIMARY KEY, val U64);"),
        ]);
        db.migrate(&migrations)?;
        
        // TODO note, u64::MAX fails because Sqlite stores ints as i64. Larger
        // requires conversion. I think it would be better to explicitly
        // disallow u64 and explicitly allow i64.
        let saved = db.save(&TestU64 {
            val: u64::MAX / 2,
            ..Default::default()
        })?;
        
        assert_eq!(saved.val, u64::MAX / 2);
        Ok(())
    }

    #[derive(Serialize, Deserialize, Default, Debug)]
    pub struct Artist {
        pub id: String,
        pub name: String,
        pub summary: Option<String>,
    }

    #[derive(Serialize, Deserialize, Default, Debug)]
    pub struct Album {
        pub id: String,
        pub title: String,
    }

    #[derive(Serialize, Deserialize, Default, Debug)]
    pub struct AlbumArtist {
        pub id: String,
        pub album_id: String,
        pub artist_id: String,
    }
}
