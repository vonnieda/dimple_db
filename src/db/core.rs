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

    /// Shortcut to create a transaction execute a query.
    /// See DbTransaction.query()
    /// TODO In (near) future, don't create a transaction, just use a read only
    /// connection.
    pub fn query<T: Entity, P: Params>(&self, sql: &str, params: P) -> Result<Vec<T>> {
        self.transaction(|t| t.query(sql, params))
    }

    /// Performs the given query, calling the closure with the results
    /// immediately and then again any time any table referenced in the query
    /// changes. Returns a QuerySubscription automatically unsubscribes the
    /// query on drop or on QuerySubscription.unsubscribe().
    pub fn query_subscribe<T: Entity, P: Params, F>(&self, sql: &str, params: P, f: F) -> Result<QuerySubscription> 
        where F: FnMut(Vec<T>) -> () {
        // run an explain query plan and extract names of tables that the query depends on
        // create a thread and subscribe to changes on the database
        // whenever a change that would affect one of the dependent tables happens
        // re-run the query and call the callback with the results
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
    /// Saves the entity to the database. The entity's type name is used for the
    /// table name, and the table columns are mapped to the entity fields using
    /// serde_rusqlite. If an entity with the same id already exists it is
    /// updated, otherwise a new entity is inserted with a new uuidv7 for it's
    /// id. A diff between the old entity, if any, and the new is created and
    /// saved in the change tracking tables. Subscribers are then notified
    /// of the changes and the newly inserted or updated entity is returned.
    pub fn save<T: Entity>(&self, entity: &T) -> Result<T> {
        let table_name = self.db.table_name_for_type::<T>()?;
        let column_names = self.db.table_column_names(self.txn, &table_name)?;
        let mut entity_value = serde_json::to_value(entity)?;
        
        let entity_id = self.ensure_entity_id(&mut entity_value)?;
        
        // Get old entity for change tracking (returns None if entity doesn't exist)
        let old_entity = self.get_entity_by_id(&table_name, &entity_id)?;
        let exists = old_entity.is_some();
        
        // Record transaction first
        self.record_transaction()?;
        
        if exists {
            self.update_entity(&table_name, &column_names, &entity_value)?;
        } else {
            self.insert_entity(&table_name, &column_names, &entity_value)?;
        }
        
        // Track changes
        self.track_changes(&table_name, &entity_id, old_entity.as_ref(), &entity_value)?;
        
        // Notify subscribers
        let event = if exists {
            DbEvent::Update(table_name.clone(), entity_id.clone())
        } else {
            DbEvent::Insert(table_name.clone(), entity_id.clone())
        };
        self.db.notify_subscribers(event);
        
        serde_json::from_value(entity_value).map_err(Into::into)
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
    
    fn get_entity_by_id(&self, table_name: &str, entity_id: &str) -> Result<Option<serde_json::Value>> {
        let sql = format!("SELECT * FROM {} WHERE id = ?", table_name);
        let results: Vec<serde_json::Value> = self.query(&sql, [entity_id])?;
        Ok(results.into_iter().next())
    }
    
    fn record_transaction(&self) -> Result<()> {
        self.txn.execute(
            "INSERT OR IGNORE INTO ZV_TRANSACTION (id, author) VALUES (?, ?)",
            [&self.id, "TODO_AUTHOR"],
        )?;
        Ok(())
    }
    
    fn track_changes(&self, table_name: &str, entity_id: &str, old_entity: Option<&serde_json::Value>, new_entity: &serde_json::Value) -> Result<()> {
        let old_map = old_entity.and_then(|e| e.as_object());
        let new_map = new_entity.as_object()
            .ok_or_else(|| anyhow::anyhow!("New entity is not an object"))?;
        
        // Track all attributes that changed
        for (key, new_value) in new_map {
            // Skip the id field since it's already stored in entity_id
            if key == "id" {
                continue;
            }
            
            let old_value = old_map.and_then(|m| m.get(key));
            
            // Skip if values are the same (including both being null)
            if old_value == Some(new_value) {
                continue;
            }
            
            // Skip if both old and new values are null (no real change)
            if old_value.map_or(true, |v| v.is_null()) && new_value.is_null() {
                continue;
            }
            
            let change_id = Uuid::now_v7().to_string();
            let old_value_str = old_value.and_then(|v| {
                if v.is_null() { None } else { Some(v.to_string()) }
            });
            let new_value_str = if new_value.is_null() { 
                None 
            } else { 
                Some(new_value.to_string()) 
            };
            
            self.txn.execute(
                "INSERT INTO ZV_CHANGE (id, transaction_id, entity_type, entity_id, attribute, old_value, new_value) 
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
                rusqlite::params![
                    &change_id,
                    &self.id,
                    table_name,
                    entity_id,
                    key,
                    old_value_str,
                    new_value_str
                ],
            )?;
        }
        
        Ok(())
    }

    pub fn query<T: Entity, P: Params>(&self, sql: &str, params: P) -> Result<Vec<T>> {
        let mut stmt = self.txn.prepare(sql)?;
        let entities = serde_rusqlite::from_rows::<T>(stmt.query(params)?)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(entities)
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
        Ok(())
    }
    
    #[test]
    fn save_with_change_tracking() -> Result<()> {
        use rusqlite_migration::{Migrations, M};
        
        let db = Db::open_memory()?;
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL, summary TEXT);"),
        ]);
        db.migrate(&migrations)?;
        
        let artist_id = db.transaction(|txn| {
            // Create new artist
            let artist = txn.save(&Artist {
                name: "Radiohead".to_string(),
                ..Default::default()
            })?;
            
            // Update the same artist
            let updated_artist = txn.save(&Artist {
                id: artist.id.clone(),
                name: "Radiohead".to_string(),
                summary: Some("English rock band".to_string()),
            })?;
            
            Ok(updated_artist.id)
        })?;
        
        // Check that changes were tracked
        let changes: Vec<(String, String, Option<String>, Option<String>)> = db.query(
            "SELECT entity_id, attribute, old_value, new_value FROM ZV_CHANGE WHERE entity_id = ? ORDER BY attribute",
            [&artist_id]
        )?;

        // Should have changes for: name (initial insert) + summary (update)
        // The id field is NOT tracked since it's already in entity_id
        // The summary field is NOT tracked for insert (null -> null is not a real change)
        // but IS tracked for update (null -> "English rock band")
        
        // Verify no id changes are tracked
        let id_changes: Vec<_> = changes.iter()
            .filter(|c| c.1 == "id")
            .collect();
        assert_eq!(id_changes.len(), 0); // No id changes should be tracked
        
        let summary_changes: Vec<_> = changes.iter()
            .filter(|c| c.1 == "summary")
            .collect();
        
        assert_eq!(summary_changes.len(), 1); // Only the update, not the insert
        
        // Update change: null -> "English rock band"
        assert_eq!(summary_changes[0].1, "summary");
        assert_eq!(summary_changes[0].2, None); // old_value was null (SQL NULL)
        assert_eq!(summary_changes[0].3, Some("\"English rock band\"".to_string())); // new_value
        
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
        let artist_id = db.transaction(|txn| {
            let artist = txn.save(&Artist {
                name: "Radiohead".to_string(),
                ..Default::default()
            })?;
            Ok(artist.id)
        })?;
        
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
        db.transaction(|txn| {
            txn.save(&Artist {
                id: artist_id.clone(),
                name: "Radiohead".to_string(),
                summary: Some("English rock band".to_string()),
            })?;
            Ok(())
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
