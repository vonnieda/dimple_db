use std::sync::{mpsc::{self, Receiver, Sender}, Arc, Mutex};

use anyhow::Result;
use r2d2::{CustomizeConnection, Pool};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{functions::FunctionFlags, Connection, Params, Transaction};
use rusqlite_migration::{Migrations};
use uuid::Uuid;

use crate::db::{query::QuerySubscription, transaction::DbTransaction, types::DbEvent, Entity};

#[derive(Clone)]
pub struct Db {
    pool: Pool<SqliteConnectionManager>,
    subscribers: Arc<Mutex<Vec<Sender<DbEvent>>>>,
}

impl Db {
    pub fn open_memory() -> Result<Self> {
        let manager = r2d2_sqlite::SqliteConnectionManager::memory();
        let pool = r2d2::Pool::builder()
            .connection_customizer(Box::new(DbConnectionCustomizer{}))
            // https://beets.io/blog/sqlite-nightmare.html
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
        let result = f(&DbTransaction::new(self, &txn))?;
        txn.commit()?;

        Ok(result)
    }

    /// Shortcut to create a transaction and save a single entity.
    /// See DbTransaction.save()
    pub fn save<T: Entity>(&self, entity: &T) -> Result<T> {
        self.transaction(|t| t.save(entity))
    }

    pub fn query<E: Entity, P: Params>(&self, sql: &str, params: P) -> Result<Vec<E>> {
        let conn = self.pool.get()?;
        let mut stmt = conn.prepare(sql)?;
        let entities = serde_rusqlite::from_rows::<E>(stmt.query(params)?)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(entities)
    }

    pub fn get<E: Entity>(&self, id: &str) -> Result<Option<E>> {
        let table_name = self.table_name_for_type::<E>()?;
        let sql = format!("SELECT * FROM {} WHERE id = ? LIMIT 1", table_name);
        Ok(self.query::<E, _>(&sql, [id])?.into_iter().next())
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
        Self::init_change_tracking_tables(&conn)?;

        let db = Db {
            pool,
            subscribers: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(db)
    }

    /// ZV is used as a prefix for the internal tables. Z puts them
    /// at the end of alphabetical lists and V differentiates them from
    /// Core Data tables.
    fn init_change_tracking_tables(conn: &Connection) -> Result<()> {
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS ZV_METADATA (
                key TEXT NOT NULL PRIMARY KEY,
                value TEXT NOT NULL
            );

            INSERT OR IGNORE INTO ZV_METADATA (key, value) 
                VALUES ('database_uuid', uuid7());

            CREATE TABLE IF NOT EXISTS ZV_TRANSACTION (
                id TEXT NOT NULL PRIMARY KEY,
                author TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ZV_CHANGE (
                id TEXT NOT NULL PRIMARY KEY,
                transaction_id TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                attribute TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                FOREIGN KEY (transaction_id) REFERENCES ZV_TRANSACTION(id)
            );
        ")?;
        Ok(())
    }

    pub(crate) fn table_name_for_type<T>(&self) -> Result<String> {
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

    use crate::db::Db;
    use crate::db::types::{ChangeTransaction, ChangeRecord, DbEvent};

    fn setup_db() -> Result<Db> {
        let db = Db::open_memory()?;
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL, summary TEXT);"),
        ]);
        db.migrate(&migrations)?;
        Ok(db)
    }

    fn get_transactions(db: &Db) -> Result<Vec<ChangeTransaction>> {
        db.query("SELECT id, author FROM ZV_TRANSACTION ORDER BY id", [])
    }

    fn get_changes(db: &Db, entity_id: &str) -> Result<Vec<ChangeRecord>> {
        db.query(
            "SELECT id, transaction_id, entity_type, entity_id, attribute, old_value, new_value 
             FROM ZV_CHANGE WHERE entity_id = ? ORDER BY attribute",
            [entity_id]
        )
    }

    fn get_changes_for_attribute(db: &Db, entity_id: &str, attribute: &str) -> Result<Vec<ChangeRecord>> {
        db.query(
            "SELECT id, transaction_id, entity_type, entity_id, attribute, old_value, new_value 
             FROM ZV_CHANGE WHERE entity_id = ? AND attribute = ? ORDER BY id",
            [entity_id, attribute]
        )
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

    // Change Tracking
    #[test]
    fn insert_creates_change_records() -> Result<()> {
        let db = setup_db()?;
        let artist = db.save(&Artist { name: "Radiohead".to_string(), ..Default::default() })?;
        
        let changes = get_changes(&db, &artist.id)?;
        assert_eq!(changes.len(), 2); // name and summary
        assert_eq!(changes[0].attribute, "name");
        assert_eq!(changes[0].old_value, None);
        assert_eq!(changes[0].new_value, Some("\"Radiohead\"".to_string()));
        assert_eq!(changes[1].attribute, "summary");
        assert_eq!(changes[1].old_value, None);
        assert_eq!(changes[1].new_value, None);
        Ok(())
    }

    #[test]
    fn update_only_tracks_modified_fields() -> Result<()> {
        let db = setup_db()?;
        let artist = db.save(&Artist { name: "Radiohead".to_string(), ..Default::default() })?;
        
        db.save(&Artist {
            id: artist.id.clone(),
            name: "Radiohead".to_string(), // unchanged
            summary: Some("Rock band".to_string()), // changed
        })?;
        
        let summary_changes = get_changes_for_attribute(&db, &artist.id, "summary")?;
        assert_eq!(summary_changes.len(), 2); // insert + update
        assert_eq!(summary_changes[1].old_value, None);
        assert_eq!(summary_changes[1].new_value, Some("\"Rock band\"".to_string()));
        Ok(())
    }

    #[test]
    fn multiple_saves_in_transaction_share_transaction_id() -> Result<()> {
        let db = setup_db()?;
        let artist = db.transaction(|txn| {
            let mut artist = txn.save(&Artist::default())?;
            artist.name = "Tool".to_string();
            txn.save(&artist)
        })?;
        
        let changes = get_changes(&db, &artist.id)?;
        let transaction_id = &changes[0].transaction_id;
        assert!(changes.iter().all(|c| c.transaction_id == *transaction_id));
        Ok(())
    }

    #[test]
    fn none_values_stored_as_sql_null() -> Result<()> {
        let db = setup_db()?;
        let artist = db.save(&Artist { 
            name: "Nirvana".to_string(), 
            summary: None, 
            ..Default::default() 
        })?;
        
        let summary_changes = get_changes_for_attribute(&db, &artist.id, "summary")?;
        assert_eq!(summary_changes.len(), 1);
        assert!(summary_changes[0].old_value.is_none());
        assert!(summary_changes[0].new_value.is_none());
        Ok(())
    }

    #[test]
    fn author_is_database_uuid() -> Result<()> {
        let db = setup_db()?;
        let _ = db.save(&Artist::default())?;
        
        let transactions = get_transactions(&db)?;
        assert_eq!(transactions.len(), 1);
        assert!(uuid::Uuid::parse_str(&transactions[0].author).is_ok());
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
        
        let artist = db.save(&Artist {
            name: "Tool".to_string(),
            summary: Some("Won't be saved".to_string()),
            ..Default::default()
        })?;
        
        let changes = get_changes(&db, &artist.id)?;
        assert_eq!(changes.len(), 1); // only name
        assert_eq!(changes[0].attribute, "name");
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

    // Change Log Query Interface
    #[test]
    fn change_structs_work_with_query() -> Result<()> {
        let db = setup_db()?;
        let artist = db.save(&Artist { name: "Pink Floyd".to_string(), ..Default::default() })?;
        
        let transactions = get_transactions(&db)?;
        let changes = get_changes(&db, &artist.id)?;
        
        assert_eq!(transactions.len(), 1);
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0].entity_type, "Artist");
        assert_eq!(changes[0].transaction_id, transactions[0].id);
        Ok(())
    }

    #[test]
    fn query_subscribe() -> Result<()> {
        let db = setup_db()?;
        let (tx, rx) = channel::<Vec<Artist>>();
        
        let _subscription = db.query_subscribe(
            "SELECT * FROM Artist", 
            (), 
            move |artists: Vec<Artist>| {
                tx.send(artists).unwrap();
            }
        )?;
        
        // Should get initial results (empty)
        let initial_results = rx.recv_timeout(Duration::from_secs(1))?;
        assert_eq!(initial_results.len(), 0);
        
        // Insert first artist
        let _artist1 = db.save(&Artist { name: "Pink Floyd".to_string(), ..Default::default() })?;
        thread::sleep(Duration::from_millis(200)); // Give time for the event to propagate
        
        // Should get updated results with 1 artist
        let results_after_first = rx.recv_timeout(Duration::from_secs(1))?;
        assert_eq!(results_after_first.len(), 1);
        assert_eq!(results_after_first[0].name, "Pink Floyd");
        
        // Insert second artist
        let _artist2 = db.save(&Artist { name: "Metallica".to_string(), ..Default::default() })?;
        thread::sleep(Duration::from_millis(200)); // Give time for the event to propagate
        
        // Should get updated results with 2 artists
        let results_after_second = rx.recv_timeout(Duration::from_secs(1))?;
        assert_eq!(results_after_second.len(), 2);
        
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
