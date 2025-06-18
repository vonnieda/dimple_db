use std::sync::{Arc, RwLock};

use rusqlite::{Connection};
use serde::Serialize;
use serde_rusqlite::*;

use crate::struct_name;

#[derive(Clone)]
pub struct Db {
    conn: Arc<RwLock<Connection>>,
}

impl Db {
    pub fn open_memory() -> anyhow::Result<Self> {
        let conn = Arc::new(RwLock::new(Connection::open_in_memory()?));
        let db = Db {
            conn,
        };
        db.init_connection()?;
        Ok(db)
    }

    // pub fn open(database_path: &str) -> Result<Self, Error> {
    //     todo!()
    // }

    fn init_connection(&self) -> anyhow::Result<()> {
        if let Ok(conn) = self.conn.write() {
            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "foreign_keys", "ON")?;
        }
        Ok(())
    }

    // pub fn register_entity<T: Serialize>(&self, entity: &T) {
    // }

    // pub fn query<T: DeserializeOwned, P: Params>(&self, sql: &str, params: P) -> Result<Vec<T>, Error> {
    //     todo!()
    // }

    // Migrate the database to the latest version defined in the slice of
    // migrations. Returns the resulting version of the database.
    // pub fn migrate(&self, migrations: &[&str]) -> Result<usize, Error> {
    // }

    // pub fn execute(&self, sql: &str) -> Result<(), Error> {
    //     self.conn.read()?.transaction()
    // }

    /// working on getting change tracking in place from save(), and then
    /// query reactivity based on the change log, and then finally sync
    /// based on the change log
    pub fn save<T: Serialize + Clone>(&self, entity: &T) -> anyhow::Result<T> {
        let name = struct_name::to_struct_name(entity)?;
        let params = to_params_named(entity)?;
        let columns = params.iter().map(|p| p.0.to_string()).collect::<Vec<_>>();
        let sql = format!("INSERT INTO {} ({}) VALUES ({})", 
            name,
            columns.iter().map(|c| c[1..].to_string()).collect::<Vec<_>>().join(","),
            columns.join(","));
        self.conn.write().unwrap().execute(&sql, params.to_slice().as_slice())?;
        Ok(entity.clone())
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::Db;

    #[derive(Serialize, Deserialize, Clone, Default, Debug)]
    pub struct Artist {
        pub key: String,
        pub name: String,
        pub disambiguation: Option<String>,
    }

    #[test]
    fn quick_start() -> anyhow::Result<()> {
        env_logger::init();
        let db = Db::open_memory()?;
        if let Ok(conn) = db.conn.write() {
            conn.execute_batch("
                CREATE TABLE _Transaction (
                    key        TEXT NOT NULL PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    actor_key  TEXT NOT NULL
                );

                CREATE TABLE _Change (
                    key             TEXT NOT NULL PRIMARY KEY,
                    created_at      TEXT NOT NULL,
                    transaction_key TEXT NOT NULL,
                    entity_name     TEXT NOT NULL,
                    entity_key      TEXT NOT NULL,
                    operation       TEXT NOT NULL, -- (insert, update, delete)
                    columns         TEXT NOT NULL  -- JSON
                );

                CREATE TABLE Artist (
                    key            TEXT NOT NULL PRIMARY KEY,
                    name           TEXT NOT NULL,
                    disambiguation TEXT
                );
            ")?;
        }
        let artist = db.save(&Artist {
            name: "Metallica".to_string(),
            ..Default::default()
        })?;
        dbg!(artist);
        Ok(())
    }
}

