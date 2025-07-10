use std::{sync::{mpsc::{self, Sender, Receiver}, Arc, RwLock, Mutex}};

use anyhow::Result;
use rusqlite::{functions::FunctionFlags, Connection, Params, Transaction};
use rusqlite_migration::{Migrations};
use uuid::Uuid;

use crate::db::{query::QuerySubscription, Entity};

#[derive(Clone)]
pub struct Db {
    conn: Arc<RwLock<Connection>>,
    subscribers: Arc<Mutex<Vec<Sender<DbEvent>>>>,
}

impl Db {
    pub fn open_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        Self::from_connection(conn)
    }

    pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let conn = Connection::open(path)?;
        Self::from_connection(conn)
    }

    pub fn migrate(&self, migrations: &Migrations) -> Result<()> {
        let mut conn = self
            .conn
            .write()
            .map_err(|_| anyhow::anyhow!("Failed to acquire write lock for migration"))?;

        migrations.to_latest(&mut *conn)?;

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
        let mut conn = self.conn.write()
            .map_err(|_| anyhow::anyhow!("Failed to acquire write lock"))?;

        let txn = conn.transaction()?;
        let result = f(&DbTransaction { 
            db: self, 
            txn: &txn,
            id: Uuid::now_v7().to_string(),
        })?;
        txn.commit()?;

        Ok(result)
    }

    /// Shortcut to create a transaction and save a single entity.
    /// See DbTransaction.save()
    pub fn save<T: Entity>(&self, entity: &T) -> Result<T> {
        self.transaction(|t| t.save(entity))
    }

    /// Shortcut to create a transaction and execute a query.
    /// See DbTransaction.query()
    /// TODO In the (near) future, don't create a transaction, just use a read
    /// only connection.
    pub fn query<T: Entity, P: Params>(&self, sql: &str, params: P) -> Result<Vec<T>> {
        self.transaction(|t| t.query(sql, params))
    }

    pub fn get<E: Entity>(&self, id: &str) -> Result<Option<E>> {
        self.transaction(|t| t.get(id))
    }

    /// Performs the given query, calling the closure with the results
    /// immediately and then again any time any table referenced in the query
    /// changes. Returns a QuerySubscription that automatically unsubscribes the
    /// query on drop or via QuerySubscription.unsubscribe().
    pub fn query_subscribe<T: Entity, P: Params, F>(&self, sql: &str, params: P, f: F) -> Result<QuerySubscription> 
        where F: FnMut(Vec<T>) -> () {
        // run an explain query plan and extract names of tables that the query depends on
        // create a thread and subscribe to changes on the database
        // whenever a change that would affect one of the dependent tables happens
        // re-run the query and call the callback with the results
        // insight: we'll need to store the query and params in the subscription
        todo!()
    } 

    fn from_connection(conn: Connection) -> Result<Self> {
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        
        conn.create_scalar_function("uuid7", 0, FunctionFlags::SQLITE_UTF8, |_ctx| {
            Ok(Uuid::now_v7().to_string())
        })?;

        Self::init_change_tracking_tables(&conn)?;

        let db = Db {
            conn: Arc::new(RwLock::new(conn)),
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

    fn table_name_for_type<T>(&self) -> Result<String> {
        let full_name = std::any::type_name::<T>();
        // Extract just the struct name from the full path
        Ok(full_name.split("::").last().unwrap_or(full_name).to_string())
    }

    fn table_column_names(&self, txn: &Transaction, table_name: &str) -> Result<Vec<String>> {
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
    
    fn notify_subscribers(&self, event: DbEvent) {
        if let Ok(mut subscribers) = self.subscribers.lock() {
            // Send to all subscribers, remove ones that fail
            subscribers.retain(|tx| {
                tx.send(event.clone()).is_ok()
            });
        }
    }
}

/// Sent to subscribers whenever the database is changed. Each variant includes
/// the entity_name and entity_id.
#[derive(Clone, Debug)]
pub enum DbEvent {
    Insert(String, String),
    Update(String, String),
    Delete(String, String),
}

pub struct DbTransaction<'a> {
    db: &'a Db,
    txn: &'a Transaction<'a>,
    id: String,
}

impl <'a> DbTransaction<'a> {
    /// Saves the entity to the database. 
    /// 
    /// The entity's type name is used for the table name, and the table
    /// columns are mapped to the entity fields using serde_rusqlite. If an
    /// entity with the same id already exists it is updated, otherwise a new
    /// entity is inserted with a new uuidv7 for it's id. 
    /// 
    /// A diff between the old entity, if any, and the new is created and
    /// saved in the change tracking tables. Subscribers are then notified
    /// of the changes and the newly inserted or updated entity is returned.
    /// 
    /// Note that only fields present in both the table and entity are mapped.
    pub fn save<E: Entity>(&self, entity: &E) -> Result<E> {
        let table_name = self.db.table_name_for_type::<E>()?;
        let column_names = self.db.table_column_names(self.txn, &table_name)?;

        let mut new_entity = serde_json::to_value(entity)?;
        let entity_id = self.ensure_entity_id(&mut new_entity)?;        
        let old_entity = self.get_entity_by_id(&table_name, &entity_id)?;
        let exists = old_entity.is_some();
        
        if exists {
            self.update_entity(&table_name, &column_names, &new_entity)?;
        } else {
            self.insert_entity(&table_name, &column_names, &new_entity)?;
        }
        
        // Track changes
        self.track_changes(&table_name, &entity_id, old_entity.as_ref(), 
            &new_entity, &column_names)?;
        
        // Notify subscribers
        let event = if exists {
            DbEvent::Update(table_name.clone(), entity_id.clone())
        } else {
            DbEvent::Insert(table_name.clone(), entity_id.clone())
        };
        self.db.notify_subscribers(event);
        
        serde_json::from_value(new_entity).map_err(Into::into)
    }

    pub fn query<E: Entity, P: Params>(&self, sql: &str, params: P) -> Result<Vec<E>> {
        let mut stmt = self.txn.prepare(sql)?;
        let entities = serde_rusqlite::from_rows::<E>(stmt.query(params)?)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(entities)
    }

    pub fn get<E: Entity>(&self, id: &str) -> Result<Option<E>> {
        let table_name = self.db.table_name_for_type::<E>()?;
        let sql = format!("SELECT * FROM {} WHERE id = ? LIMIT 1", table_name);
        Ok(self.query::<E, _>(&sql, [id])?.into_iter().next())
    }

    fn ensure_entity_id(&self, entity_value: &mut serde_json::Value) -> Result<String> {
        match entity_value.get("id").and_then(|v| v.as_str()) {
            Some(id) if !id.is_empty() => Ok(id.to_string()),
            _ => {
                let new_id = Uuid::now_v7().to_string();
                entity_value["id"] = serde_json::Value::String(new_id.clone());
                Ok(new_id)
            }
        }
    }
    
    fn update_entity(&self, table_name: &str, column_names: &[String], entity_value: &serde_json::Value) -> Result<()> {
        let set_clause = column_names
            .iter()
            .filter(|col| *col != "id")
            .map(|col| format!("{} = :{}", col, col))
            .collect::<Vec<_>>()
            .join(", ");
        
        let sql = format!("UPDATE {} SET {} WHERE id = :id", table_name, set_clause);
        self.execute_with_named_params(&sql, entity_value)
    }
    
    fn insert_entity(&self, table_name: &str, column_names: &[String], entity_value: &serde_json::Value) -> Result<()> {
        let placeholders = column_names
            .iter()
            .map(|col| format!(":{}", col))
            .collect::<Vec<_>>()
            .join(", ");
        
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table_name,
            column_names.join(", "),
            placeholders
        );
        self.execute_with_named_params(&sql, entity_value)
    }
    
    fn execute_with_named_params(&self, sql: &str, entity_value: &serde_json::Value) -> Result<()> {
        let mut stmt = self.txn.prepare(sql)?;
        let params = serde_rusqlite::to_params_named(entity_value)?;
        stmt.execute(params.to_slice().as_slice())?;
        Ok(())
    }
    
    // TODO this entire thing is sus, what's with the filtering?
    // it should just be query and return first
    fn get_entity_by_id(&self, table_name: &str, entity_id: &str) -> Result<Option<serde_json::Value>> {
        let column_names = self.db.table_column_names(self.txn, table_name)?;
        let sql = format!("SELECT * FROM {} WHERE id = ?", table_name);
        let mut stmt = self.txn.prepare(&sql)?;
        
        let mut rows = stmt.query([entity_id])?;
        if let Some(row) = rows.next()? {
            let mut entity = serde_json::Map::new();
            for (i, column_name) in column_names.iter().enumerate() {
                let value: Option<String> = row.get(i)?;
                entity.insert(
                    column_name.clone(), 
                    match value {
                        Some(val) => serde_json::Value::String(val),
                        None => serde_json::Value::Null,
                    }
                );
            }
            Ok(Some(serde_json::Value::Object(entity)))
        } else {
            Ok(None)
        }
    }

    // TODO rewrite
    fn track_changes(&self, table_name: &str, entity_id: &str, 
            old_entity: Option<&serde_json::Value>, 
            new_entity: &serde_json::Value,
            column_names: &[String]) -> Result<()> {
        
    
        // First, ensure we have a transaction record
        self.txn.execute(
            "INSERT OR IGNORE INTO ZV_TRANSACTION (id, author) VALUES (?, ?)",
            [&self.id, "system"]
        )?;
        
        // Track changes for each column (except id)
        for column_name in column_names {
            if column_name == "id" {
                continue;
            }
            
            // TODO check what's going on with this null stuff
            let old_value = old_entity
                .and_then(|e| e.get(column_name))
                .map(|v| match v {
                    serde_json::Value::Null => None,
                    _ => Some(v.to_string())
                })
                .flatten();
                
            let new_value = new_entity
                .get(column_name)
                .map(|v| match v {
                    serde_json::Value::Null => None,
                    _ => Some(v.to_string())
                })
                .flatten();
            
            // Track all values on insert, only changes on update
            let is_insert = old_entity.is_none();
            let should_track = is_insert || (old_value != new_value);
            if should_track {
                let change_id = Uuid::now_v7().to_string();
                
                // TODO why
                let old_val_str = old_value.as_deref().unwrap_or("NULL");
                let new_val_str = new_value.as_deref().unwrap_or("NULL");
                
                self.txn.execute(
                    "INSERT INTO ZV_CHANGE (id, transaction_id, entity_type, entity_id, attribute, old_value, new_value) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    [
                        &change_id,
                        &self.id,
                        table_name,
                        entity_id,
                        column_name,
                        old_val_str,
                        new_val_str,
                    ]
                )?;
            }
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use serde::{Deserialize, Serialize};

    use crate::db::Db;

    #[test]
    fn open_memory() -> Result<()> {
        let _ = Db::open_memory()?;
        Ok(())
    }

    #[test]
    fn type_name() -> Result<()> {
        let db = Db::open_memory()?;
        assert!(db.table_name_for_type::<Artist>()? == "Artist");
        assert!(db.table_name_for_type::<Album>()? == "Album");
        assert!(db.table_name_for_type::<AlbumArtist>()? == "AlbumArtist");
        Ok(())
    }

    #[test]
    fn save() -> Result<()> {
        use rusqlite_migration::{Migrations, M};
        
        let db = Db::open_memory()?;
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL, summary TEXT);"),
        ]);
        db.migrate(&migrations)?;
        
        let artist = db.transaction(|txn| {
            let mut artist = txn.save(&Artist::default())?;
            artist.name = "Deftones".to_string();
            let artist = txn.save(&artist)?;
            Ok(artist)
        })?;
        assert!(uuid::Uuid::parse_str(&artist.id).is_ok());
        assert_eq!(artist.name, "Deftones");
        
        // Verify that both saves happened in the same transaction
        let transaction_ids: Vec<String> = db.transaction(|txn| {
            txn.query("SELECT DISTINCT transaction_id FROM ZV_CHANGE WHERE entity_id = ?", [&artist.id])
        })?;
        
        // Should have exactly one transaction_id for all changes to this entity
        assert_eq!(transaction_ids.len(), 1, "All changes should belong to the same transaction");
        
        // Verify we have the expected number of changes (name field changed from "" to "Deftones")
        let change_count: i64 = db.transaction(|txn| {
            let mut stmt = txn.txn.prepare("SELECT COUNT(*) FROM ZV_CHANGE WHERE entity_id = ?")?;
            let count = stmt.query_row([&artist.id], |row| row.get(0))?;
            Ok(count)
        })?;
        
        // Should have 3 changes: 
        // 1. Initial insert: name (NULL -> "")
        // 2. Initial insert: summary (NULL -> NULL) 
        // 3. Update: name ("" -> "Deftones")
        assert_eq!(change_count, 3, "Should have 3 changes tracked for this entity");
        
        // Most importantly: verify that the name field has 2 changes with the same transaction_id
        let name_changes: Vec<(String, String, String)> = db.transaction(|txn| {
            txn.query("SELECT transaction_id, old_value, new_value FROM ZV_CHANGE WHERE entity_id = ? AND attribute = 'name' ORDER BY id", [&artist.id])
        })?;
        
        assert_eq!(name_changes.len(), 2, "Should have 2 changes to the name field");
        assert_eq!(name_changes[0].0, name_changes[1].0, "Both name changes should have the same transaction_id");
        assert_eq!(name_changes[0].1, "NULL", "First change should be from NULL");
        assert_eq!(name_changes[0].2, "\"\"", "First change should be to empty string");
        assert_eq!(name_changes[1].1, "\"\"", "Second change should be from empty string");
        assert_eq!(name_changes[1].2, "\"Deftones\"", "Second change should be to 'Deftones'");
        
        Ok(())
    }

    #[test]
    fn change_tracking() -> Result<()> {
        use rusqlite_migration::{Migrations, M};
        
        let db = Db::open_memory()?;
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL, summary TEXT);"),
        ]);
        db.migrate(&migrations)?;

        // Test 1: Insert new entity creates ZV_CHANGEs for each column (except id)
        let artist_id = db.save(&Artist {
            name: "Radiohead".to_string(),
            summary: Some("English rock band".to_string()),
            ..Default::default()
        })?.id;

        // Check that a transaction was created
        let txn_count: i64 = db.transaction(|txn| {
            let mut stmt = txn.txn.prepare("SELECT COUNT(*) FROM ZV_TRANSACTION")?;
            let count = stmt.query_row([], |row| row.get(0))?;
            Ok(count)
        })?;
        assert_eq!(txn_count, 1);

        // Check that changes were recorded for name and summary (but not id)
        let changes: Vec<(String, String, String, String)> = db.transaction(|txn| {
            txn.query("SELECT attribute, old_value, new_value, entity_id FROM ZV_CHANGE ORDER BY attribute", [])
        })?;
        
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0].0, "name"); // attribute
        assert_eq!(changes[0].1, "NULL"); // old_value
        assert_eq!(changes[0].2, "\"Radiohead\""); // new_value
        assert_eq!(changes[0].3, artist_id); // entity_id
        
        assert_eq!(changes[1].0, "summary"); // attribute
        assert_eq!(changes[1].1, "NULL"); // old_value
        assert_eq!(changes[1].2, "\"English rock band\""); // new_value
        assert_eq!(changes[1].3, artist_id); // entity_id

        // Test 2: Update entity creates changes only for modified fields
        db.save(&Artist {
            id: artist_id.clone(),
            name: "Radiohead".to_string(), // unchanged
            summary: Some("Alternative rock band".to_string()), // changed
        })?;

        // Should now have 2 transactions
        let txn_count: i64 = db.transaction(|txn| {
            let mut stmt = txn.txn.prepare("SELECT COUNT(*) FROM ZV_TRANSACTION")?;
            let count = stmt.query_row([], |row| row.get(0))?;
            Ok(count)
        })?;
        assert_eq!(txn_count, 2);

        // Should have 3 total changes (2 from insert + 1 from update)
        let change_count: i64 = db.transaction(|txn| {
            let mut stmt = txn.txn.prepare("SELECT COUNT(*) FROM ZV_CHANGE")?;
            let count = stmt.query_row([], |row| row.get(0))?;
            Ok(count)
        })?;
        assert_eq!(change_count, 3);

        // Check the latest change is for summary
        let latest_change: (String, String, String) = db.transaction(|txn| {
            let mut stmt = txn.txn.prepare("SELECT attribute, old_value, new_value FROM ZV_CHANGE ORDER BY id DESC LIMIT 1")?;
            let row = stmt.query_row([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?
                ))
            })?;
            Ok(row)
        })?;
        
        assert_eq!(latest_change.0, "summary");
        assert_eq!(latest_change.1, "\"English rock band\"");
        assert_eq!(latest_change.2, "\"Alternative rock band\"");

        Ok(())
    }

    #[test]
    fn get_entity() -> Result<()> {
        use rusqlite_migration::{Migrations, M};
        
        let db = Db::open_memory()?;
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL, summary TEXT);"),
        ]);
        db.migrate(&migrations)?;
        
        // Save an artist
        let saved_artist = db.save(&Artist {
            name: "The Beatles".to_string(),
            summary: Some("British rock band".to_string()),
            ..Default::default()
        })?;
        
        // Test getting an existing entity
        let retrieved_artist: Option<Artist> = db.get(&saved_artist.id)?;
        
        assert!(retrieved_artist.is_some());
        let artist = retrieved_artist.unwrap();
        assert_eq!(artist.id, saved_artist.id);
        assert_eq!(artist.name, "The Beatles");
        assert_eq!(artist.summary, Some("British rock band".to_string()));
        
        // Test getting a non-existent entity
        let non_existent: Option<Artist> = db.get("non-existent-id")?;
        
        assert!(non_existent.is_none());
        
        Ok(())
    }
        
    #[test]
    fn subscriber_notifications() -> Result<()> {
        use rusqlite_migration::{Migrations, M};
        use std::time::Duration;
        use crate::db::core::DbEvent;
        
        let db = Db::open_memory()?;
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL, summary TEXT);"),
        ]);
        db.migrate(&migrations)?;
        
        // Subscribe to events
        let receiver = db.subscribe();
        
        // Create an artist (should trigger Insert event)
        let artist_id = db.save(&Artist {
            name: "Radiohead".to_string(),
            ..Default::default()
        })?.id;
        
        // Check for Insert event
        let event = receiver.recv_timeout(Duration::from_millis(100))?;
        match event {
            DbEvent::Insert(table_name, entity_id) => {
                assert_eq!(table_name, "Artist");
                assert_eq!(entity_id, artist_id);
            }
            _ => panic!("Expected Insert event, got {:?}", event),
        }
        
        // Update the artist (should trigger Update event)
        db.save(&Artist {
            id: artist_id.clone(),
            name: "Radiohead".to_string(),
            summary: Some("English rock band".to_string()),
        })?;
        
        // Check for Update event
        let event = receiver.recv_timeout(Duration::from_millis(100))?;
        match event {
            DbEvent::Update(table_name, entity_id) => {
                assert_eq!(table_name, "Artist");
                assert_eq!(entity_id, artist_id);
            }
            _ => panic!("Expected Update event, got {:?}", event),
        }
        
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
