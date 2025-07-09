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
        let result = f(&DbTransaction { db: self, txn: &txn })?;
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
        todo!()
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
}

impl <'a> DbTransaction<'a> {
    /// Saves the entity to the database. The entity's type name is used for the
    /// table name, and the table columns are mapped to the entity fields using
    /// serde_rusqlite. If an entity with the same id already exists it is
    /// updated, otherwise a new entity is inserted with a new uuidv7 for it's
    /// id. A diff between the old entity, if any, and the new is created and
    /// saved in the change tracking tables. Subscribers are then notified
    /// of the changes and the newly inserted or updated entity is returned.
    /// 
    /// TODO This will need a transaction id for the ZV_TRANSACTION table, or
    /// maybe this should just be in DbTransaction.
    pub fn save<T: Entity>(&self, entity: &T) -> Result<T> {
        // let table_name = self.type_name::<T>();
        // get the table's column names
        // map the entity's fields to columns
        // insert or update the value, creating a uuidv7 id if needed
        // record the change in history tables
        // notify listeners (maybe with the transaction id)
        //   ideally this could be triggered by the writes to the history tables
        //   and really the information saved there is the core of what the
        //   listeners need to know
        //   leads pretty logically to having a context that knows its place in
        //   the changelog
        let table_name = self.db.table_name_for_type::<T>()?;
        let column_names = self.db.table_column_names(self.txn, &table_name)?;
        let entity_value = serde_json::to_value(entity)?;
        let entity_id = // TODO get the id field from entity_value, or create a new id
        todo!()
    }

    pub fn query<T: Entity, P: Params>(&self, sql: &str, params: P) -> Result<Vec<T>> {
        // run the query, use serde_rusqlite to convert back to entities
        todo!()
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
        let db = Db::open_memory()?;
        let artist = db.transaction(|txn| {
            let mut artist = txn.save(&Artist::default())?;
            artist.name = "Deftones".to_string();
            let artist = txn.save(&artist)?;
            Ok(artist)
        })?;
        assert!(uuid::Uuid::parse_str(&artist.id).is_ok());
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
