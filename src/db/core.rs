use std::{sync::{mpsc::{self, Sender, Receiver}, Arc, RwLock, Mutex}};

use anyhow::Result;
use rusqlite::{functions::FunctionFlags, Connection, Params, Transaction};
use rusqlite_migration::{Migrations};
use serde::{Deserialize, Serialize};
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

/// Represents a transaction record in the ZV_TRANSACTION table
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChangeTransaction {
    pub id: String,
    pub author: String,
}

/// Represents a change record in the ZV_CHANGE table
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChangeRecord {
    pub id: String,
    pub transaction_id: String,
    pub entity_type: String,
    pub entity_id: String,
    pub attribute: String,
    pub old_value: Option<String>,
    pub new_value: Option<String>,
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

        // Convert the entity to a JSON Value so we can manipulate it
        // generically without needing more than Serialize.
        let mut new_value = serde_json::to_value(entity)?;
        let id = self.ensure_entity_id(&mut new_value)?;        
        let old_value = self.get::<E>(&id)?
            .and_then(|e| serde_json::to_value(e).ok());

        let exists = old_value.is_some();
        
        if exists {
            self.update_entity(&table_name, &column_names, &new_value)?;
        } else {
            self.insert_entity(&table_name, &column_names, &new_value)?;
        }
        
        // Track changes
        self.track_changes(&table_name, &id, old_value.as_ref(), 
            &new_value, &column_names)?;
        
        // Notify subscribers
        let event = if exists {
            DbEvent::Update(table_name.clone(), id.clone())
        } else {
            DbEvent::Insert(table_name.clone(), id.clone())
        };
        self.db.notify_subscribers(event);
        
        self.get::<E>(&id)?
            .ok_or_else(|| anyhow::anyhow!("Failed to retrieve saved entity"))    
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
        self.execute_with_named_params(&sql, entity_value, column_names)
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
        self.execute_with_named_params(&sql, entity_value, column_names)
    }
    
    fn execute_with_named_params(&self, sql: &str, entity_value: &serde_json::Value, column_names: &[String]) -> Result<()> {
        let mut stmt = self.txn.prepare(sql)?;
        let str_refs: Vec<&str> = column_names.iter().map(|s| s.as_str()).collect();
        let params = serde_rusqlite::to_params_named_with_fields(entity_value, &str_refs)?;
        stmt.execute(params.to_slice().as_slice())?;
        Ok(())
    }
    
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
            
            // Convert JSON null to None, other values to their string representation
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
                
                // Use rusqlite's support for Option to store NULL values properly
                self.txn.execute(
                    "INSERT INTO ZV_CHANGE (id, transaction_id, entity_type, entity_id, attribute, old_value, new_value) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    rusqlite::params![
                        &change_id,
                        &self.id,
                        table_name,
                        entity_id,
                        column_name,
                        old_value.as_deref(),
                        new_value.as_deref(),
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

    // TODO test that structs with Optional fields store None values as SQL NULL
    // in the database and change log.
    // TODO test that multiple save() calls in a transaction create one transaction
    // and multiple change records.

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
        let name_changes: Vec<(String, Option<String>, String)> = db.transaction(|txn| {
            txn.query("SELECT transaction_id, old_value, new_value FROM ZV_CHANGE WHERE entity_id = ? AND attribute = 'name' ORDER BY id", [&artist.id])
        })?;
        
        assert_eq!(name_changes.len(), 2, "Should have 2 changes to the name field");
        assert_eq!(name_changes[0].0, name_changes[1].0, "Both name changes should have the same transaction_id");
        assert!(name_changes[0].1.is_none(), "First change should be from NULL");
        assert_eq!(name_changes[0].2, "\"\"", "First change should be to empty string");
        assert_eq!(name_changes[1].1, Some("\"\"".to_string()), "Second change should be from empty string");
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
        let changes: Vec<(String, Option<String>, String, String)> = db.transaction(|txn| {
            txn.query("SELECT attribute, old_value, new_value, entity_id FROM ZV_CHANGE ORDER BY attribute", [])
        })?;
        
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0].0, "name"); // attribute
        assert!(changes[0].1.is_none()); // old_value should be NULL
        assert_eq!(changes[0].2, "\"Radiohead\""); // new_value
        assert_eq!(changes[0].3, artist_id); // entity_id
        
        assert_eq!(changes[1].0, "summary"); // attribute
        assert!(changes[1].1.is_none()); // old_value should be NULL
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
    fn extra_struct_fields_not_in_change_log() -> Result<()> {
        use rusqlite_migration::{Migrations, M};
        
        let db = Db::open_memory()?;
        // Create table with only id and name fields (no summary field)
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL);"),
        ]);
        db.migrate(&migrations)?;
        
        // Save artist with a summary field that doesn't exist in the table
        // The Artist struct has a summary field, but the table doesn't
        let artist = db.save(&Artist {
            name: "Tool".to_string(),
            summary: Some("This field doesn't exist in the table".to_string()),
            ..Default::default()
        })?;
        
        // Verify the artist was saved successfully
        assert!(!artist.id.is_empty());
        assert_eq!(artist.name, "Tool");
        // The returned entity only has fields that exist in the table, so summary is None
        assert_eq!(artist.summary, None);
        
        // Query all changes
        let changes: Vec<(String, String)> = db.transaction(|txn| {
            txn.query("SELECT attribute, new_value FROM ZV_CHANGE WHERE entity_id = ?", [&artist.id])
        })?;
        
        // Should only have 1 change (for name), not for summary
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].0, "name");
        assert_eq!(changes[0].1, "\"Tool\"");
        
        // Verify no change was recorded for the summary field
        let summary_changes: Vec<String> = db.transaction(|txn| {
            txn.query("SELECT attribute FROM ZV_CHANGE WHERE entity_id = ? AND attribute = 'summary'", [&artist.id])
        })?;
        assert!(summary_changes.is_empty(), "No changes should be recorded for fields not in the table");
        
        // Also test update scenario
        db.save(&Artist {
            id: artist.id.clone(),
            name: "Tool".to_string(), // unchanged
            summary: Some("Still not in table".to_string()), // this won't be saved
        })?;
        
        // Should have no new changes since name didn't change and summary isn't in table
        let all_changes: Vec<String> = db.transaction(|txn| {
            txn.query("SELECT attribute FROM ZV_CHANGE WHERE entity_id = ?", [&artist.id])
        })?;
        assert_eq!(all_changes.len(), 1, "Should still only have the original name change");
        
        // Verify that when we read the entity back, the summary field is None
        let retrieved: Option<Artist> = db.get(&artist.id)?;
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.name, "Tool");
        assert_eq!(retrieved.summary, None, "Summary should be None when read from DB since column doesn't exist");
        
        Ok(())
    }

    #[test]
    fn change_log_null_values() -> Result<()> {
        use rusqlite_migration::{Migrations, M};
        
        let db = Db::open_memory()?;
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL, summary TEXT);"),
        ]);
        db.migrate(&migrations)?;
        
        // Save artist with NULL summary
        let artist = db.save(&Artist {
            name: "Nirvana".to_string(),
            summary: None,
            ..Default::default()
        })?;
        
        // Query the raw change values to see how NULL is stored
        let null_check: Vec<(Option<String>, Option<String>)> = db.transaction(|txn| {
            let mut stmt = txn.txn.prepare(
                "SELECT old_value, new_value FROM ZV_CHANGE WHERE entity_id = ? AND attribute = 'summary'"
            )?;
            let result = stmt.query_map([&artist.id], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
            Ok(result)
        })?;
        
        assert_eq!(null_check.len(), 1);
        
        // Check if we're storing SQL NULL correctly
        match &null_check[0] {
            (old_val, new_val) => {
                println!("Old value: {:?}", old_val);
                println!("New value: {:?}", new_val);
                
                // These should be None (SQL NULL), not Some("NULL")
                assert!(old_val.is_none(), 
                    "Old value should be SQL NULL (None) but was {:?}", old_val);
                assert!(new_val.is_none(), 
                    "New value should be SQL NULL (None) but was {:?}", new_val);
            }
        }
        
        // Update to non-null value
        db.save(&Artist {
            id: artist.id.clone(),
            name: "Nirvana".to_string(),
            summary: Some("Grunge band".to_string()),
        })?;
        
        // Check the update change
        let update_change: Vec<(Option<String>, Option<String>)> = db.transaction(|txn| {
            let mut stmt = txn.txn.prepare(
                "SELECT old_value, new_value FROM ZV_CHANGE 
                 WHERE entity_id = ? AND attribute = 'summary' 
                 ORDER BY id DESC LIMIT 1"
            )?;
            let result = stmt.query_map([&artist.id], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
            Ok(result)
        })?;
        
        assert_eq!(update_change.len(), 1);
        match &update_change[0] {
            (old_val, new_val) => {
                // Old should be SQL NULL, new should be the JSON string
                assert!(old_val.is_none(), 
                    "Old value should be SQL NULL (None) but was {:?}", old_val);
                assert!(new_val.is_some() && new_val.as_ref().unwrap() == "\"Grunge band\"", 
                    "New value should be '\"Grunge band\"' but was {:?}", new_val);
            }
        }
        
        // Update back to NULL
        db.save(&Artist {
            id: artist.id.clone(),
            name: "Nirvana".to_string(),
            summary: None,
        })?;
        
        // Check if NULL is stored correctly
        let null_update: Vec<(Option<String>, Option<String>)> = db.transaction(|txn| {
            let mut stmt = txn.txn.prepare(
                "SELECT old_value, new_value FROM ZV_CHANGE 
                 WHERE entity_id = ? AND attribute = 'summary' 
                 ORDER BY id DESC LIMIT 1"
            )?;
            let result = stmt.query_map([&artist.id], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
            Ok(result)
        })?;
        
        match &null_update[0] {
            (old_val, new_val) => {
                // Old should be the JSON string, new should be SQL NULL
                assert!(old_val.is_some() && old_val.as_ref().unwrap() == "\"Grunge band\"", 
                    "Old value should be '\"Grunge band\"' but was {:?}", old_val);
                assert!(new_val.is_none(), 
                    "New value should be SQL NULL (None) but was {:?}", new_val);
            }
        }
        
        Ok(())
    }

    #[test]
    fn query_change_log_with_structs() -> Result<()> {
        use rusqlite_migration::{Migrations, M};
        use crate::db::core::{ChangeTransaction, ChangeRecord};
        
        let db = Db::open_memory()?;
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL, summary TEXT);"),
        ]);
        db.migrate(&migrations)?;
        
        // Save a new artist
        let artist = db.save(&Artist {
            name: "Pink Floyd".to_string(),
            summary: Some("Progressive rock band".to_string()),
            ..Default::default()
        })?;
        
        // Query transactions using the new struct and Db::query
        let transactions: Vec<ChangeTransaction> = db.query(
            "SELECT id, author FROM ZV_TRANSACTION ORDER BY id", 
            []
        )?;
        
        // Should have one transaction
        assert_eq!(transactions.len(), 1);
        assert_eq!(transactions[0].author, "system");
        assert!(uuid::Uuid::parse_str(&transactions[0].id).is_ok());
        
        // Query changes using the new struct and Db::query
        let changes: Vec<ChangeRecord> = db.query(
            "SELECT id, transaction_id, entity_type, entity_id, attribute, old_value, new_value 
             FROM ZV_CHANGE 
             WHERE entity_id = ? 
             ORDER BY attribute",
            [&artist.id]
        )?;
        
        // Should have 2 changes (name and summary)
        assert_eq!(changes.len(), 2);
        
        // Verify name change
        let name_change = changes.iter().find(|c| c.attribute == "name").unwrap();
        assert_eq!(name_change.entity_type, "Artist");
        assert_eq!(name_change.entity_id, artist.id);
        assert_eq!(name_change.old_value, None);
        assert_eq!(name_change.new_value, Some("\"Pink Floyd\"".to_string()));
        assert_eq!(name_change.transaction_id, transactions[0].id);
        
        // Verify summary change
        let summary_change = changes.iter().find(|c| c.attribute == "summary").unwrap();
        assert_eq!(summary_change.entity_type, "Artist");
        assert_eq!(summary_change.entity_id, artist.id);
        assert_eq!(summary_change.old_value, None);
        assert_eq!(summary_change.new_value, Some("\"Progressive rock band\"".to_string()));
        assert_eq!(summary_change.transaction_id, transactions[0].id);
        
        // Update the artist
        db.save(&Artist {
            id: artist.id.clone(),
            name: "Pink Floyd".to_string(), // unchanged
            summary: Some("Legendary progressive rock band".to_string()), // changed
        })?;
        
        // Query all transactions again using Db::query
        let all_transactions: Vec<ChangeTransaction> = db.query(
            "SELECT id, author FROM ZV_TRANSACTION ORDER BY id", 
            []
        )?;
        
        // Should now have 2 transactions
        assert_eq!(all_transactions.len(), 2);
        
        // Query only the changes from the update
        let update_changes: Vec<ChangeRecord> = db.query(
            "SELECT id, transaction_id, entity_type, entity_id, attribute, old_value, new_value 
             FROM ZV_CHANGE 
             WHERE entity_id = ? AND transaction_id = ?",
            (&artist.id, &all_transactions[1].id)
        )?;
        
        // Should have 1 change (only summary was updated)
        assert_eq!(update_changes.len(), 1);
        assert_eq!(update_changes[0].attribute, "summary");
        assert_eq!(update_changes[0].old_value, Some("\"Progressive rock band\"".to_string()));
        assert_eq!(update_changes[0].new_value, Some("\"Legendary progressive rock band\"".to_string()));
        
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
