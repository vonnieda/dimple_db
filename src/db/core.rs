use std::sync::{Arc, RwLock};

use rusqlite::{Connection, Params};
use rusqlite_migration::{Migrations};
use uuid::Uuid;

use crate::{db::DbTransaction, Entity};

#[derive(Clone)]
pub struct Db {
    conn: Arc<RwLock<Connection>>,
    database_uuid: String,
}

impl Db {
    pub fn open_memory() -> anyhow::Result<Self> {
        let conn = Connection::open_in_memory()?;
        Self::from_connection(conn)
    }

    pub fn open<P: AsRef<std::path::Path>>(path: P) -> anyhow::Result<Self> {
        let conn = Connection::open(path)?;
        Self::from_connection(conn)
    }

    pub fn migrate(&self, migrations: &Migrations) -> anyhow::Result<()> {
        let mut conn = self
            .conn
            .write()
            .map_err(|_| anyhow::anyhow!("Failed to acquire write lock for migration"))?;

        migrations.to_latest(&mut *conn)?;

        Ok(())
    }

    pub fn save<T: Entity>(&self, entity: &T) -> anyhow::Result<T> {
        let table_name = self.type_name::<T>();
        // create a transaction
        // get the table's column names
        // map the entity's fields to columns
        // insert or update the value, creating a uuidv7 id if needed
        // record the change in history tables
        // notify listeners
        todo!()
    }

    pub fn query<T: Entity, P: Params>(&self, sql: &str, params: P) -> anyhow::Result<Vec<T>> {
        // run the query, use serde_rusqlite to convert back to entities
        todo!()
    }

    pub fn query_one<T: Entity, P: Params>(&self, sql: &str, params: P) -> anyhow::Result<Option<T>> {
        // shortcut for query().first()
        todo!()
    }

    pub fn query_subscribe<T: Entity, P: Params, F: FnMut(Vec<T>) -> ()>(&self, sql: &str, params: P, cb: F) -> anyhow::Result<()> {
        // run an explain query plan and extract names of tables that the query depends on
        // and then when there are changes in any of those tables, re-run the query and
        // call the callback with the results
        // I want to implement this in terms of a higher level event system, like
        // before.
        todo!()
    }

    pub fn delete<T: Entity>(&self, entity: &T) -> anyhow::Result<()> {
        todo!()
    }

    fn from_connection(conn: Connection) -> anyhow::Result<Self> {
        let db = Db {
            conn: Arc::new(RwLock::new(conn)),
            database_uuid: "".to_string(), // Will be set after initialization
        };
        db.init_connection()?;
        db.init_change_tracking_tables()?;
        let database_uuid = db.get_or_create_database_uuid()?;
        
        let mut db = db;
        db.database_uuid = database_uuid;
        Ok(db)
    }

    fn init_connection(&self) -> anyhow::Result<()> {
        let conn = self
            .conn
            .write()
            .map_err(|_| anyhow::anyhow!("Failed to acquire write lock for connection init"))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        
        self.register_uuid7_function(&conn)?;

        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS _metadata (
                key TEXT NOT NULL PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS _transaction (
                id TEXT NOT NULL PRIMARY KEY,
                timestamp INTEGER NOT NULL,
                author TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS _change (
                transaction_id TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_key TEXT NOT NULL,
                attribute TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                PRIMARY KEY (transaction_id, entity_type, entity_key, attribute),
                FOREIGN KEY (transaction_id) REFERENCES _transaction(id)
            );

            CREATE INDEX IF NOT EXISTS idx_change_entity ON _change(entity_type, entity_key);
            CREATE INDEX IF NOT EXISTS idx_transaction_timestamp ON _transaction(timestamp);
        ",
        )?;

        
        Ok(())
    }

    fn register_uuid7_function(&self, conn: &Connection) -> anyhow::Result<()> {
        use rusqlite::functions::FunctionFlags;
        
        conn.create_scalar_function(
            "uuid7",
            0, // No parameters
            FunctionFlags::SQLITE_UTF8,
            |_ctx| {
                Ok(uuid::Uuid::now_v7().to_string())
            },
        )?;
        
        Ok(())
    }

    fn init_change_tracking_tables(&self) -> anyhow::Result<()> {
        let conn = self.conn.write().map_err(|_| {
            anyhow::anyhow!("Failed to acquire write lock for change tracking init")
        })?;
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS _metadata (
                key TEXT NOT NULL PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS _transaction (
                id TEXT NOT NULL PRIMARY KEY,
                timestamp INTEGER NOT NULL,
                author TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS _change (
                transaction_id TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_key TEXT NOT NULL,
                attribute TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                PRIMARY KEY (transaction_id, entity_type, entity_key, attribute),
                FOREIGN KEY (transaction_id) REFERENCES _transaction(id)
            );

            CREATE INDEX IF NOT EXISTS idx_change_entity ON _change(entity_type, entity_key);
            CREATE INDEX IF NOT EXISTS idx_transaction_timestamp ON _transaction(timestamp);
        ",
        )?;
        Ok(())
    }

    fn get_or_create_database_uuid(&self) -> anyhow::Result<String> {
        let mut conn = self
            .conn
            .write()
            .map_err(|_| anyhow::anyhow!("Failed to acquire write lock"))?;

        let tx = conn.transaction()?;
        
        // Try insert with a new UUID - will fail silently if key already exists
        let new_uuid = Uuid::now_v7().to_string();
        let _ = tx.execute(
            "INSERT OR IGNORE INTO _metadata (key, value) VALUES ('database_uuid', ?)",
            [&new_uuid],
        );

        // Now get the UUID (either the existing one or the one we just inserted)
        let uuid = tx.query_row(
            "SELECT value FROM _metadata WHERE key = 'database_uuid'",
            [],
            |row| row.get::<_, String>(0),
        )?;

        tx.commit()?;
        Ok(uuid)
    }

    pub fn get_database_uuid(&self) -> &str {
        &self.database_uuid
    }

    fn type_name<T>(&self) -> String {
        let full_name = std::any::type_name::<T>();
        // Extract just the struct name from the full path
        full_name.split("::").last().unwrap_or(full_name).to_string()
    }
}

#[cfg(test)]
mod tests {

}
