use std::sync::{Arc, RwLock};

use anyhow::Result;
use rusqlite::{functions::FunctionFlags, Connection, Params, Transaction};
use rusqlite_migration::{Migrations};
use uuid::Uuid;

use crate::db::Entity;

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

    pub fn transaction<F, R>(&self, f: F) -> Result<R>
        where F: FnOnce(&Transaction) -> Result<R> {
        let mut conn = self.conn.write()
            .map_err(|_| anyhow::anyhow!("Failed to acquire write lock"))?;

        let txn = conn.transaction()?;
        let result = f(&txn)?;
        txn.commit()?;

        Ok(result)
    }

    pub fn save<T: Entity>(&self, txn: &Transaction, entity: &T) -> Result<T> {
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
        todo!()
    }

    pub fn query<T: Entity, P: Params>(&self, txn: &Transaction, sql: &str, params: P) -> Result<Vec<T>> {
        // run the query, use serde_rusqlite to convert back to entities
        todo!()
    }

    pub fn query_one<T: Entity, P: Params>(&self, txn: &Transaction, sql: &str, params: P) -> Result<Option<T>> {
        // shortcut for query().first()
        todo!()
    }

    pub fn query_subscribe<T: Entity, P: Params, F>(&self, sql: &str, params: P, f: F) -> Result<()> 
        where F: FnMut(Vec<T>) -> () {
        // run an explain query plan and extract names of tables that the query depends on
        // and then when there are changes in any of those tables, re-run the query and
        // call the callback with the results
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
                FOREIGN KEY (transaction_id) REFERENCES ZV_TRANSACTION(id)
            );
        ")?;
        Ok(())
    }

    pub fn type_name<T>() -> String {
        let full_name = std::any::type_name::<T>();
        // Extract just the struct name from the full path
        full_name.split("::").last().unwrap_or(full_name).to_string()
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
        assert!(Db::type_name::<Artist>() == "Artist");
        assert!(Db::type_name::<Album>() == "Album");
        assert!(Db::type_name::<AlbumArtist>() == "AlbumArtist");
        Ok(())
    }

    #[test]
    fn save() -> Result<()> {
        let db = Db::open_memory()?;
        let artist = db.transaction(|txn| {
            let mut artist = db.save(txn, &Artist::default())?;
            artist.name = "Deftones".to_string();
            let artist = db.save(txn, &artist)?;
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
