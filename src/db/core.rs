use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use rusqlite::Connection;
use rusqlite_migration::{M, Migrations};
use uuid::Uuid;

use crate::db::ChangeType;
use crate::notifier::Notifier;
use crate::Entity;
use super::types::QuerySubscription;

#[derive(Clone)]
pub struct Db {
    pub(crate) conn: Arc<RwLock<Connection>>,
    pub(crate) database_uuid: String,
    pub(crate) subscriptions: Arc<RwLock<HashMap<String, QuerySubscription>>>,
    pub(crate) query_notifier: Arc<Notifier<serde_json::Value>>, // Generic JSON notifications
}

impl Db {
    pub fn open_memory() -> anyhow::Result<Self> {
        let conn = Arc::new(RwLock::new(Connection::open_in_memory()?));
        let db = Db {
            conn,
            database_uuid: "".to_string(), // Will be set after initialization
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            query_notifier: Arc::new(Notifier::new()),
        };
        db.init_connection()?;
        db.init_change_tracking_tables()?;
        let database_uuid = db.get_or_create_database_uuid()?;
        let mut db = db;
        db.database_uuid = database_uuid;
        Ok(db)
    }

    pub fn open<P: AsRef<std::path::Path>>(path: P) -> anyhow::Result<Self> {
        let conn = Arc::new(RwLock::new(Connection::open(path)?));
        let db = Db {
            conn,
            database_uuid: "".to_string(), // Will be set after initialization
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            query_notifier: Arc::new(Notifier::new()),
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
                author TEXT NOT NULL,
                bundle_id TEXT
            );

            CREATE TABLE IF NOT EXISTS _change (
                id TEXT NOT NULL PRIMARY KEY,
                transaction_id TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_key TEXT NOT NULL,
                change_type TEXT NOT NULL,
                old_values TEXT,
                new_values TEXT,
                FOREIGN KEY (transaction_id) REFERENCES _transaction(id)
            );

            CREATE INDEX IF NOT EXISTS idx_change_transaction_id ON _change(transaction_id);
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

        // Use a transaction to ensure atomicity
        let tx = conn.transaction()?;

        // Try to get existing UUID
        let existing_uuid = tx.query_row(
            "SELECT value FROM _metadata WHERE key = 'database_uuid'",
            [],
            |row| row.get::<_, String>(0),
        );

        match existing_uuid {
            Ok(uuid) => {
                tx.commit()?;
                Ok(uuid)
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                // Create new UUID and store it
                let new_uuid = Uuid::now_v7().to_string();

                tx.execute(
                    "INSERT INTO _metadata (key, value) VALUES ('database_uuid', ?)",
                    [&new_uuid],
                )?;

                tx.commit()?;
                Ok(new_uuid)
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn migrate_sql(&self, migration_sqls: &[&str]) -> anyhow::Result<()> {
        let mut conn = self
            .conn
            .write()
            .map_err(|_| anyhow::anyhow!("Failed to acquire write lock for migration"))?;

        // Convert string slices to M::up() migrations
        let migrations: Vec<M> = migration_sqls.iter().map(|sql| M::up(sql)).collect();

        let migrations = Migrations::new(migrations);
        migrations.to_latest(&mut *conn)?;

        Ok(())
    }

    pub fn migrate(&self, migrations: &Migrations) -> anyhow::Result<()> {
        let mut conn = self
            .conn
            .write()
            .map_err(|_| anyhow::anyhow!("Failed to acquire write lock for migration"))?;

        migrations.to_latest(&mut *conn)?;

        Ok(())
    }

    pub fn get_database_uuid(&self) -> &str {
        &self.database_uuid
    }

    pub fn save<T: Entity>(&self, entity: &T) -> anyhow::Result<T> {
        let table_name = self.struct_name(entity)?;

        let mut entity_json = serde_json::to_value(entity)?;
        self.ensure_entity_has_key(&mut entity_json)?;

        let mut conn = self
            .conn
            .write()
            .map_err(|_| anyhow::anyhow!("Failed to acquire write lock"))?;
        let tx = conn.transaction()?;

        let table_columns = self.get_table_columns(&tx, &table_name)?;
        let all_params = serde_rusqlite::to_params_named(&entity_json)?;

        let key_value = self.extract_key_value_from_json(&entity_json)?;
        let exists = self.record_exists(&tx, &table_name, &key_value)?;

        // Capture old values for change tracking
        let old_values = if exists {
            Some(self.get_record_as_json(&tx, &table_name, &key_value)?)
        } else {
            None
        };

        let change_type = if exists {
            self.update_record(&tx, &table_name, &key_value, all_params, &table_columns)?;
            ChangeType::Update
        } else {
            self.insert_record(&tx, &table_name, all_params, &table_columns)?;
            ChangeType::Insert
        };

        // Record change
        let new_values = serde_json::to_string(&entity_json)?;
        self.record_change(
            &tx,
            &table_name,
            &key_value,
            change_type,
            old_values,
            Some(new_values),
        )?;

        tx.commit()?;

        // Release the write lock before triggering notifications
        drop(conn);

        // Trigger reactive query notifications after successful commit and lock release
        self.notify_query_subscribers(&table_name, &key_value)?;

        // Convert back to T for return
        let final_entity: T = serde_json::from_value(entity_json)?;
        Ok(final_entity)
    }

    pub(crate) fn struct_name<T>(&self, _value: &T) -> anyhow::Result<String> {
        let full_name = std::any::type_name::<T>();

        // Extract just the struct name from the full path
        // e.g. "dimple_data::db::tests::Artist" -> "Artist"
        let name = full_name.split("::").last().unwrap_or(full_name);

        Ok(name.to_string())
    }

    pub(crate) fn get_table_columns(
        &self,
        conn: &rusqlite::Connection,
        table_name: &str,
    ) -> anyhow::Result<Vec<String>> {
        let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", table_name))?;
        let column_iter = stmt.query_map([], |row| {
            let column_name: String = row.get(1)?; // Column name is at index 1
            Ok(column_name)
        })?;

        let mut columns = Vec::new();
        for column in column_iter {
            columns.push(column?);
        }

        if columns.is_empty() {
            return Err(anyhow::anyhow!(
                "Table '{}' not found or has no columns",
                table_name
            ));
        }

        Ok(columns)
    }

    pub(crate) fn record_exists(
        &self,
        tx: &rusqlite::Transaction,
        table_name: &str,
        key: &str,
    ) -> anyhow::Result<bool> {
        let sql = format!("SELECT 1 FROM {} WHERE key = ? LIMIT 1", table_name);
        Ok(tx.prepare(&sql)?.exists([key])?)
    }

    pub(crate) fn ensure_entity_has_key(
        &self,
        entity_json: &mut serde_json::Value,
    ) -> anyhow::Result<()> {
        // Generate a key if it doesn't exist or is empty
        let needs_key = match entity_json.get("key") {
            Some(key) => key.is_null() || (key.is_string() && key.as_str() == Some("")),
            None => true,
        };

        if needs_key {
            entity_json["key"] = serde_json::Value::String(Uuid::now_v7().to_string());
        }

        Ok(())
    }

    pub(crate) fn extract_key_value_from_json(
        &self,
        entity_json: &serde_json::Value,
    ) -> anyhow::Result<String> {
        entity_json
            .get("key")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("Entity missing required 'key' field"))
    }

    pub(crate) fn get_record_as_json(
        &self,
        tx: &rusqlite::Transaction,
        table_name: &str,
        key: &str,
    ) -> anyhow::Result<String> {
        let sql = format!("SELECT * FROM {} WHERE key = ?", table_name);
        let mut stmt = tx.prepare(&sql)?;
        let mut rows = stmt.query([key])?;

        if let Some(row) = rows.next()? {
            // Use serde_rusqlite to convert row to JSON Value, then to string
            let json_value: serde_json::Value = serde_rusqlite::from_row(row)?;
            Ok(serde_json::to_string(&json_value)?)
        } else {
            Err(anyhow::anyhow!("Record not found"))
        }
    }

    pub(crate) fn update_record(
        &self,
        tx: &rusqlite::Transaction,
        table_name: &str,
        key: &str,
        all_params: serde_rusqlite::NamedParamSlice,
        table_columns: &[String],
    ) -> anyhow::Result<()> {
        // Filter to only include columns that exist in the table and are not the key
        let filtered_params: Vec<(String, &dyn rusqlite::ToSql)> = all_params
            .iter()
            .filter_map(|(name, value)| {
                let column_name = name.strip_prefix(':').unwrap_or(name);
                if column_name != "key" && table_columns.iter().any(|col| col == column_name) {
                    Some((column_name.to_string(), value.as_ref()))
                } else {
                    None
                }
            })
            .collect();

        if filtered_params.is_empty() {
            return Ok(()); // Nothing to update
        }

        // Build single UPDATE statement with all columns
        let set_clauses: Vec<String> = filtered_params
            .iter()
            .map(|(name, _)| format!("{} = ?", name))
            .collect();

        let sql = format!(
            "UPDATE {} SET {} WHERE key = ?",
            table_name,
            set_clauses.join(", ")
        );
        log::debug!("SQL EXECUTE: {}", sql);

        let mut stmt = tx.prepare(&sql)?;

        // Build parameter list: all non-key values + key for WHERE clause
        let mut values: Vec<&dyn rusqlite::ToSql> =
            filtered_params.iter().map(|(_, value)| *value).collect();
        values.push(&key);

        let affected_rows = stmt.execute(&values[..])?;
        log::debug!("SQL EXECUTE RESULT: {} rows affected", affected_rows);
        Ok(())
    }

    pub(crate) fn insert_record(
        &self,
        tx: &rusqlite::Transaction,
        table_name: &str,
        all_params: serde_rusqlite::NamedParamSlice,
        table_columns: &[String],
    ) -> anyhow::Result<()> {
        // Filter to only include columns that exist in the table
        let filtered_params: Vec<(String, &dyn rusqlite::ToSql)> = all_params
            .iter()
            .filter_map(|(name, value)| {
                let column_name = name.strip_prefix(':').unwrap_or(name);
                if table_columns.iter().any(|col| col == column_name) {
                    Some((column_name.to_string(), value.as_ref()))
                } else {
                    None
                }
            })
            .collect();

        if filtered_params.is_empty() {
            return Err(anyhow::anyhow!(
                "No valid columns found for table '{}'",
                table_name
            ));
        }

        let column_names: Vec<&str> = filtered_params
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        let placeholders = vec!["?"; filtered_params.len()].join(", ");

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table_name,
            column_names.join(", "),
            placeholders
        );
        log::debug!("SQL EXECUTE: {}", sql);

        let mut stmt = tx.prepare(&sql)?;
        let values: Vec<&dyn rusqlite::ToSql> =
            filtered_params.iter().map(|(_, value)| *value).collect();
        let affected_rows = stmt.execute(&values[..])?;
        log::debug!("SQL EXECUTE RESULT: {} rows affected", affected_rows);

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_database_uuid_persistence() -> anyhow::Result<()> {
        use serde::{Deserialize, Serialize};

        #[derive(Serialize, Deserialize, Clone, Default, Debug)]
        pub struct Artist {
            pub key: String,
            pub name: String,
            pub disambiguation: Option<String>,
        }

        let db = crate::Db::open_memory()?;

        // Save an artist to ensure change tracking works
        if let Ok(conn) = db.conn.write() {
            conn.execute_batch(
                "
                CREATE TABLE Artist (
                    key            TEXT NOT NULL PRIMARY KEY,
                    name           TEXT NOT NULL,
                    disambiguation TEXT
                );
            ",
            )?;
        }

        let artist = db.save(&Artist {
            name: "Metallica".to_string(),
            ..Default::default()
        })?;

        // Query changes - should have 1 since tracking is always enabled
        let changes = db.get_changes_for_entity("Artist", &artist.key)?;
        assert_eq!(changes.len(), 1);

        // Check that author is a valid UUID
        assert!(!db.database_uuid.is_empty());
        assert!(uuid::Uuid::parse_str(&db.database_uuid).is_ok());

        Ok(())
    }

    #[test]
    fn test_schema_migration() -> anyhow::Result<()> {
        let db = crate::Db::open_memory()?;

        // Define migrations
        let migrations = &[
            // Migration 1: Create users table
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT
            );",
            // Migration 2: Add age column
            "ALTER TABLE users ADD COLUMN age INTEGER;",
            // Migration 3: Create posts table
            "CREATE TABLE posts (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                content TEXT,
                FOREIGN KEY (user_id) REFERENCES users(id)
            );",
        ];

        // Run migrations
        db.migrate_sql(migrations)?;

        // Verify tables were created
        let conn = db.conn.read().unwrap();

        // Check users table structure
        let mut stmt = conn.prepare("PRAGMA table_info(users)")?;
        let column_info: Vec<(i32, String, String)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
            .collect::<Result<Vec<_>, _>>()?;

        // Should have id, name, email, age columns
        assert_eq!(column_info.len(), 4);
        assert_eq!(column_info[0].1, "id");
        assert_eq!(column_info[1].1, "name");
        assert_eq!(column_info[2].1, "email");
        assert_eq!(column_info[3].1, "age");

        // Check posts table exists
        let mut stmt = conn.prepare("PRAGMA table_info(posts)")?;
        let posts_columns: Vec<String> = stmt
            .query_map([], |row| Ok(row.get::<_, String>(1)?))?
            .collect::<Result<Vec<_>, _>>()?;

        assert!(posts_columns.contains(&"id".to_string()));
        assert!(posts_columns.contains(&"user_id".to_string()));
        assert!(posts_columns.contains(&"title".to_string()));
        assert!(posts_columns.contains(&"content".to_string()));

        // Test that we can insert data
        conn.execute(
            "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
            ["John Doe", "john@example.com", "30"],
        )?;

        conn.execute(
            "INSERT INTO posts (user_id, title, content) VALUES (?, ?, ?)",
            ["1", "Hello World", "This is my first post"],
        )?;

        // Verify data was inserted
        let user_count: i64 = conn.query_row("SELECT COUNT(*) FROM users", [], |row| row.get(0))?;
        let post_count: i64 = conn.query_row("SELECT COUNT(*) FROM posts", [], |row| row.get(0))?;

        assert_eq!(user_count, 1);
        assert_eq!(post_count, 1);

        Ok(())
    }

    #[test]
    fn test_migration_idempotency() -> anyhow::Result<()> {
        let db = crate::Db::open_memory()?;

        let migrations = &[
            "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT);",
            "ALTER TABLE test_table ADD COLUMN email TEXT;",
        ];

        // Run migrations twice - should not fail
        db.migrate_sql(migrations)?;
        db.migrate_sql(migrations)?;

        // Verify table structure is correct
        let conn = db.conn.read().unwrap();
        let mut stmt = conn.prepare("PRAGMA table_info(test_table)")?;
        let columns: Vec<String> = stmt
            .query_map([], |row| Ok(row.get::<_, String>(1)?))?
            .collect::<Result<Vec<_>, _>>()?;

        assert_eq!(columns.len(), 3);
        assert!(columns.contains(&"id".to_string()));
        assert!(columns.contains(&"name".to_string()));
        assert!(columns.contains(&"email".to_string()));

        Ok(())
    }
}