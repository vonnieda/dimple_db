use std::{sync::{mpsc::Receiver, Arc, RwLock}};

use anyhow::Result;
use rusqlite::{functions::FunctionFlags, Connection, Params, Transaction};
use rusqlite_migration::{Migrations};
use uuid::Uuid;

use crate::db::{query::QuerySubscription, Entity};

#[derive(Clone)]
pub struct Db {
    conn: Arc<RwLock<Connection>>,
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
    /// Dropped Receivers will be lazily cleaned up.
    pub fn subscribe(&self) -> Receiver<DbEvent> {
        todo!()
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
                entity_key TEXT NOT NULL,
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
}

/// Sent to subscribers whenever the database is changed. Each variant includes
/// the entity_name and entity_id.
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
        let exists = self.entity_exists(&table_name, &entity_id)?;
        
        if exists {
            self.update_entity(&table_name, &column_names, &entity_value)?;
        } else {
            self.insert_entity(&table_name, &column_names, &entity_value)?;
        }
        
        // TODO: Implement change tracking in ZV_CHANGE table
        // TODO: Notify subscribers
        
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
    
    fn entity_exists(&self, table_name: &str, entity_id: &str) -> Result<bool> {
        self.txn.query_row(
            &format!("SELECT EXISTS(SELECT 1 FROM {} WHERE id = ?)", table_name),
            [entity_id],
            |row| row.get(0)
        ).map_err(Into::into)
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
