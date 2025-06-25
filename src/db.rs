use std::collections::{HashMap, HashSet};
use std::sync::mpsc::Receiver;
use std::sync::{Arc, RwLock, Weak};

use rusqlite::Connection;
use rusqlite_migration::{M, Migrations};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use uuid::Uuid;

use crate::notifier::Notifier;

pub trait Entity: Serialize + DeserializeOwned {}

// Blanket implementation for any type that meets the requirements
impl<T> Entity for T where T: Serialize + DeserializeOwned {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ChangeType {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub id: String,
    pub timestamp: i64,
    pub author: String,
    pub bundle_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Change {
    pub id: String,
    pub transaction_id: String,
    pub entity_type: String,
    pub entity_key: String,
    pub change_type: String,
    pub old_values: Option<String>,
    pub new_values: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult<T> {
    pub data: Vec<T>,
    pub keys: HashSet<String>,
    pub hash: u64,
}

#[derive(Debug, Clone)]
pub struct QuerySubscription {
    pub id: String,
    pub sql: String,
    pub params: Vec<String>, // Serialized parameters
    pub dependent_tables: HashSet<String>,
    pub last_result_keys: HashSet<String>,
    pub last_result_hash: u64,
}

#[derive(Debug, Clone)]
pub struct QueryChange<T> {
    pub subscription_id: String,
    pub new_result: QueryResult<T>,
}

pub struct QueryObserver {
    subscription_id: String,
    db: Weak<RwLock<HashMap<String, QuerySubscription>>>,
}

impl Drop for QueryObserver {
    fn drop(&mut self) {
        if let Some(subscriptions) = self.db.upgrade() {
            if let Ok(mut subs) = subscriptions.write() {
                subs.remove(&self.subscription_id);
            }
        }
    }
}

pub struct QuerySubscriber<T> {
    subscription_id: String,
    db: Weak<RwLock<HashMap<String, QuerySubscription>>>,
    _receiver: Receiver<QueryResult<T>>,
}

impl<T> Drop for QuerySubscriber<T> {
    fn drop(&mut self) {
        if let Some(subscriptions) = self.db.upgrade() {
            if let Ok(mut subs) = subscriptions.write() {
                subs.remove(&self.subscription_id);
            }
        }
    }
}

impl<T> QuerySubscriber<T> {
    pub fn recv(&self) -> Result<QueryResult<T>, std::sync::mpsc::RecvError> {
        self._receiver.recv()
    }

    pub fn recv_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<QueryResult<T>, std::sync::mpsc::RecvTimeoutError> {
        self._receiver.recv_timeout(timeout)
    }

    pub fn try_recv(&self) -> Result<QueryResult<T>, std::sync::mpsc::TryRecvError> {
        self._receiver.try_recv()
    }

    pub fn iter(&self) -> std::sync::mpsc::Iter<QueryResult<T>> {
        self._receiver.iter()
    }
}

#[derive(Clone)]
pub struct Db {
    conn: Arc<RwLock<Connection>>,
    author: String,
    subscriptions: Arc<RwLock<HashMap<String, QuerySubscription>>>,
    query_notifier: Arc<Notifier<serde_json::Value>>, // Generic JSON notifications
}

impl Db {
    pub fn open_memory() -> anyhow::Result<Self> {
        let conn = Arc::new(RwLock::new(Connection::open_in_memory()?));
        let db = Db {
            conn,
            author: "".to_string(), // Will be set after initialization
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            query_notifier: Arc::new(Notifier::new()),
        };
        db.init_connection()?;
        db.init_change_tracking_tables()?;
        let author = db.get_or_create_database_uuid()?;
        let mut db = db;
        db.author = author;
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
        let conn = self
            .conn
            .read()
            .map_err(|_| anyhow::anyhow!("Failed to acquire read lock"))?;

        // Try to get existing UUID
        let existing_uuid = conn.query_row(
            "SELECT value FROM _metadata WHERE key = 'database_uuid'",
            [],
            |row| row.get::<_, String>(0),
        );

        match existing_uuid {
            Ok(uuid) => Ok(uuid),
            Err(_) => {
                // Create new UUID and store it
                drop(conn); // Release read lock
                let conn = self
                    .conn
                    .write()
                    .map_err(|_| anyhow::anyhow!("Failed to acquire write lock"))?;
                let new_uuid = Uuid::now_v7().to_string();

                conn.execute(
                    "INSERT INTO _metadata (key, value) VALUES ('database_uuid', ?)",
                    [&new_uuid],
                )?;

                Ok(new_uuid)
            }
        }
    }

    pub fn query<T: Entity>(
        &self,
        sql: &str,
        params: &[&dyn rusqlite::ToSql],
    ) -> anyhow::Result<Vec<T>> {
        log::debug!("SQL QUERY: {}", sql);

        let conn = self
            .conn
            .read()
            .map_err(|_| anyhow::anyhow!("Failed to acquire read lock"))?;

        let mut stmt = conn.prepare(sql)?;
        let mut rows = stmt.query(params)?;

        let mut results = Vec::new();
        while let Some(row) = rows.next()? {
            let entity = serde_rusqlite::from_row::<T>(row)?;
            results.push(entity);
        }

        log::debug!("SQL QUERY RESULT: {} rows", results.len());
        Ok(results)
    }

    pub fn save<T: Entity>(&self, entity: &T) -> anyhow::Result<T> {
        let table_name = self.struct_name(entity)?;

        // Convert to JSON once at the start
        let mut entity_json = serde_json::to_value(entity)?;
        self.ensure_entity_has_key_json(&mut entity_json)?;

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

        // Record change - reuse the JSON string
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

    fn ensure_entity_has_key_json(
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

    fn extract_key_value_from_json(
        &self,
        entity_json: &serde_json::Value,
    ) -> anyhow::Result<String> {
        entity_json
            .get("key")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("Entity missing required 'key' field"))
    }

    fn record_exists(
        &self,
        tx: &rusqlite::Transaction,
        table_name: &str,
        key: &str,
    ) -> anyhow::Result<bool> {
        let sql = format!("SELECT 1 FROM {} WHERE key = ? LIMIT 1", table_name);
        Ok(tx.prepare(&sql)?.exists([key])?)
    }

    fn update_record(
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

    fn insert_record(
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

    fn get_table_columns(
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

    pub fn struct_name<T>(&self, _value: &T) -> anyhow::Result<String> {
        let full_name = std::any::type_name::<T>();

        // Extract just the struct name from the full path
        // e.g. "dimple_data::db::tests::Artist" -> "Artist"
        let name = full_name.split("::").last().unwrap_or(full_name);

        Ok(name.to_string())
    }

    fn get_record_as_json(
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

    fn record_change(
        &self,
        tx: &rusqlite::Transaction,
        entity_type: &str,
        entity_key: &str,
        change_type: ChangeType,
        old_values: Option<String>,
        new_values: Option<String>,
    ) -> anyhow::Result<()> {
        // Create transaction record
        let transaction_id = Uuid::now_v7().to_string();
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as i64;

        log::debug!(
            "SQL EXECUTE: INSERT INTO _transaction (id, timestamp, author, bundle_id) VALUES (?, ?, ?, ?)"
        );
        let tx_affected = tx.execute(
            "INSERT INTO _transaction (id, timestamp, author, bundle_id) VALUES (?, ?, ?, ?)",
            rusqlite::params![
                transaction_id,
                timestamp,
                self.author,
                Option::<String>::None
            ],
        )?;
        log::debug!("SQL EXECUTE RESULT: {} rows affected", tx_affected);

        // Create change record
        let change_id = Uuid::now_v7().to_string();
        let change_type_str = match change_type {
            ChangeType::Insert => "Insert",
            ChangeType::Update => "Update",
            ChangeType::Delete => "Delete",
        };

        log::debug!(
            "SQL EXECUTE: INSERT INTO _change (id, transaction_id, entity_type, entity_key, change_type, old_values, new_values) VALUES (?, ?, ?, ?, ?, ?, ?)"
        );
        let ch_affected = tx.execute(
            "INSERT INTO _change (id, transaction_id, entity_type, entity_key, change_type, old_values, new_values) VALUES (?, ?, ?, ?, ?, ?, ?)",
            rusqlite::params![
                change_id,
                transaction_id,
                entity_type,
                entity_key,
                change_type_str,
                old_values,
                new_values,
            ],
        )?;
        log::debug!("SQL EXECUTE RESULT: {} rows affected", ch_affected);

        Ok(())
    }

    pub fn get_changes_since(&self, timestamp: i64) -> anyhow::Result<Vec<Change>> {
        self.query(
            "SELECT c.id, c.transaction_id, c.entity_type, c.entity_key, c.change_type, c.old_values, c.new_values 
             FROM _change c 
             JOIN _transaction t ON c.transaction_id = t.id 
             WHERE t.timestamp > ? 
             ORDER BY t.timestamp ASC",
            &[&timestamp],
        )
    }

    pub fn get_changes_since_uuid(&self, since_uuid: &str) -> anyhow::Result<Vec<Change>> {
        self.query(
            "SELECT c.id, c.transaction_id, c.entity_type, c.entity_key, c.change_type, c.old_values, c.new_values 
             FROM _change c 
             JOIN _transaction t ON c.transaction_id = t.id 
             WHERE t.id > ? 
             ORDER BY t.id ASC",
            &[&since_uuid],
        )
    }

    pub fn get_changes_for_entity(
        &self,
        entity_type: &str,
        entity_key: &str,
    ) -> anyhow::Result<Vec<Change>> {
        self.query(
            "SELECT c.id, c.transaction_id, c.entity_type, c.entity_key, c.change_type, c.old_values, c.new_values 
             FROM _change c 
             JOIN _transaction t ON c.transaction_id = t.id 
             WHERE c.entity_type = ? AND c.entity_key = ? 
             ORDER BY t.timestamp ASC",
            &[&entity_type, &entity_key],
        )
    }

    pub fn get_changes_by_author(&self, author: &str) -> anyhow::Result<Vec<Change>> {
        self.query(
            "SELECT c.id, c.transaction_id, c.entity_type, c.entity_key, c.change_type, c.old_values, c.new_values 
             FROM _change c 
             JOIN _transaction t ON c.transaction_id = t.id 
             WHERE t.author = ? 
             ORDER BY t.timestamp ASC",
            &[&author],
        )
    }

    pub fn get_all_changes(&self) -> anyhow::Result<Vec<Change>> {
        self.query(
            "SELECT c.id, c.transaction_id, c.entity_type, c.entity_key, c.change_type, c.old_values, c.new_values 
             FROM _change c 
             JOIN _transaction t ON c.transaction_id = t.id 
             ORDER BY t.timestamp ASC",
            &[],
        )
    }

    pub fn apply_changes(&self, changes: &[Change]) -> anyhow::Result<()> {
        self.apply_remote_changes(changes)
    }

    pub fn apply_remote_changes(&self, changes: &[Change]) -> anyhow::Result<()> {
        // This is a fallback that shouldn't be used for new code
        // Use apply_remote_changes_with_author instead
        self.apply_remote_changes_with_author(changes, "unknown")
    }

    pub fn apply_remote_changes_with_author(
        &self,
        changes: &[Change],
        remote_author: &str,
    ) -> anyhow::Result<()> {
        let mut conn = self
            .conn
            .write()
            .map_err(|_| anyhow::anyhow!("Failed to acquire write lock"))?;
        let tx = conn.transaction()?;

        // Group changes by transaction to preserve transaction information
        let mut transactions_to_insert = std::collections::HashMap::new();
        let mut changes_to_apply = Vec::new();

        for change in changes {
            // Check if we already have this transaction
            if let Ok(existing_transaction) =
                self.get_transaction_by_id(&tx, &change.transaction_id)
            {
                // Skip changes from our own author to avoid conflicts
                if existing_transaction.author == self.author {
                    continue;
                }
                // Transaction already exists, just queue the change for application
                changes_to_apply.push(change);
            } else {
                // Transaction doesn't exist, we need to create it with original author info
                // Extract timestamp from UUIDv7 transaction ID
                let timestamp = if let Ok(uuid) = uuid::Uuid::parse_str(&change.transaction_id) {
                    if uuid.get_version() == Some(uuid::Version::SortRand) {
                        // Extract timestamp from UUIDv7 (first 48 bits are milliseconds since Unix epoch)
                        let bytes = uuid.as_bytes();
                        let timestamp_ms = u64::from_be_bytes([
                            0, 0, // pad to 8 bytes
                            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5],
                        ]);
                        timestamp_ms as i64
                    } else {
                        // Fallback to current time
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as i64
                    }
                } else {
                    // Fallback to current time
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as i64
                };

                let transaction = Transaction {
                    id: change.transaction_id.clone(),
                    timestamp,
                    author: remote_author.to_string(), // Preserve original author
                    bundle_id: None,
                };
                transactions_to_insert.insert(change.transaction_id.clone(), transaction);
                changes_to_apply.push(change);
            }
        }

        // Insert new transactions
        for (_, transaction) in transactions_to_insert {
            log::debug!(
                "SQL EXECUTE: INSERT OR IGNORE INTO _transaction (id, timestamp, author, bundle_id) VALUES (?, ?, ?, ?)"
            );
            let affected_rows = tx.execute(
                "INSERT OR IGNORE INTO _transaction (id, timestamp, author, bundle_id) VALUES (?, ?, ?, ?)",
                rusqlite::params![transaction.id, transaction.timestamp, transaction.author, transaction.bundle_id],
            )?;
            log::debug!("SQL EXECUTE RESULT: {} rows affected", affected_rows);
        }

        // Insert change records and apply entity changes
        for change in changes_to_apply {
            // Insert the change record (preserve original change ID and transaction ID)
            log::debug!(
                "SQL EXECUTE: INSERT OR IGNORE INTO _change (id, transaction_id, entity_type, entity_key, change_type, old_values, new_values) VALUES (?, ?, ?, ?, ?, ?, ?)"
            );
            let affected_rows = tx.execute(
                "INSERT OR IGNORE INTO _change (id, transaction_id, entity_type, entity_key, change_type, old_values, new_values) VALUES (?, ?, ?, ?, ?, ?, ?)",
                rusqlite::params![
                    change.id,
                    change.transaction_id,
                    change.entity_type,
                    change.entity_key,
                    change.change_type,
                    change.old_values,
                    change.new_values,
                ],
            )?;
            log::debug!("SQL EXECUTE RESULT: {} rows affected", affected_rows);

            // Apply the entity change
            match change.change_type.as_str() {
                "Insert" => self.apply_insert_change(&tx, change)?,
                "Update" => self.apply_update_change(&tx, change)?,
                "Delete" => self.apply_delete_change(&tx, change)?,
                _ => {
                    return Err(anyhow::anyhow!(
                        "Unknown change type: {}",
                        change.change_type
                    ));
                }
            }
        }

        tx.commit()?;
        Ok(())
    }

    fn get_transaction_by_id(
        &self,
        conn: &rusqlite::Connection,
        transaction_id: &str,
    ) -> anyhow::Result<Transaction> {
        let mut stmt =
            conn.prepare("SELECT id, timestamp, author, bundle_id FROM _transaction WHERE id = ?")?;
        let mut rows = stmt.query([transaction_id])?;

        if let Some(row) = rows.next()? {
            Ok(Transaction {
                id: row.get(0)?,
                timestamp: row.get(1)?,
                author: row.get(2)?,
                bundle_id: row.get(3)?,
            })
        } else {
            Err(anyhow::anyhow!("Transaction not found: {}", transaction_id))
        }
    }

    fn apply_insert_change(
        &self,
        tx: &rusqlite::Transaction,
        change: &Change,
    ) -> anyhow::Result<()> {
        if let Some(new_values) = &change.new_values {
            let entity_json: serde_json::Value = serde_json::from_str(new_values)?;

            // Check if the record already exists
            if self.record_exists(tx, &change.entity_type, &change.entity_key)? {
                // Record exists, this might be a conflict. For now, skip it.
                log::warn!(
                    "Insert conflict: {} {} already exists, skipping",
                    change.entity_type,
                    change.entity_key
                );
                return Ok(());
            }

            // Insert the record
            let table_columns = self.get_table_columns(tx, &change.entity_type)?;
            let all_params = serde_rusqlite::to_params_named(&entity_json)?;
            self.insert_record(tx, &change.entity_type, all_params, &table_columns)?;
        }
        Ok(())
    }

    fn apply_update_change(
        &self,
        tx: &rusqlite::Transaction,
        change: &Change,
    ) -> anyhow::Result<()> {
        if let Some(new_values) = &change.new_values {
            let entity_json: serde_json::Value = serde_json::from_str(new_values)?;

            // Check if the record exists
            if !self.record_exists(tx, &change.entity_type, &change.entity_key)? {
                // Record doesn't exist, treat as insert
                log::warn!(
                    "Update target missing: {} {}, treating as insert",
                    change.entity_type,
                    change.entity_key
                );
                return self.apply_insert_change(tx, change);
            }

            // Update the record
            let table_columns = self.get_table_columns(tx, &change.entity_type)?;
            let all_params = serde_rusqlite::to_params_named(&entity_json)?;
            self.update_record(
                tx,
                &change.entity_type,
                &change.entity_key,
                all_params,
                &table_columns,
            )?;
        }
        Ok(())
    }

    fn apply_delete_change(
        &self,
        tx: &rusqlite::Transaction,
        change: &Change,
    ) -> anyhow::Result<()> {
        // Check if the record exists
        if !self.record_exists(tx, &change.entity_type, &change.entity_key)? {
            // Record doesn't exist, nothing to delete
            log::warn!(
                "Delete target missing: {} {}, skipping",
                change.entity_type,
                change.entity_key
            );
            return Ok(());
        }

        // Delete the record
        let sql = format!("DELETE FROM {} WHERE key = ?", change.entity_type);
        tx.execute(&sql, [&change.entity_key])?;
        Ok(())
    }

    pub fn get_transactions_since(&self, timestamp: i64) -> anyhow::Result<Vec<Transaction>> {
        self.query(
            "SELECT id, timestamp, author, bundle_id 
             FROM _transaction 
             WHERE timestamp > ? 
             ORDER BY timestamp ASC",
            &[&timestamp],
        )
    }

    pub fn get_latest_uuid_for_author(&self, author: &str) -> anyhow::Result<String> {
        let conn = self
            .conn
            .read()
            .map_err(|_| anyhow::anyhow!("Failed to acquire read lock"))?;
        // Get the latest transaction ID (UUIDv7) for this author
        // UUIDv7 are sortable, so MAX() will give us the most recent
        let mut stmt = conn.prepare("SELECT MAX(id) FROM _transaction WHERE author = ?")?;
        let mut rows = stmt.query([author])?;

        if let Some(row) = rows.next()? {
            let uuid: Option<String> = row.get(0)?;
            Ok(uuid.unwrap_or_else(|| "00000000-0000-0000-0000-000000000000".to_string()))
        } else {
            Ok("00000000-0000-0000-0000-000000000000".to_string())
        }
    }

    fn extract_table_dependencies(
        &self,
        sql: &str,
        params: &[&dyn rusqlite::ToSql],
    ) -> anyhow::Result<HashSet<String>> {
        let conn = self
            .conn
            .read()
            .map_err(|_| anyhow::anyhow!("Failed to acquire read lock"))?;

        let explain_sql = format!("EXPLAIN QUERY PLAN {}", sql);
        let mut stmt = conn.prepare(&explain_sql)?;
        let mut rows = stmt.query(params)?;

        let mut tables = HashSet::new();

        while let Some(row) = rows.next()? {
            // EXPLAIN QUERY PLAN columns: id, parent, notused, detail
            let detail: String = row.get(3)?;

            // Parse table names from the detail column
            // Examples: "SCAN TABLE Artist", "SEARCH TABLE Artist USING INDEX"
            if let Some(table_name) = self.extract_table_from_detail(&detail) {
                tables.insert(table_name);
            }
        }
        Ok(tables)
    }

    fn extract_table_from_detail(&self, detail: &str) -> Option<String> {
        // Handle common patterns in EXPLAIN QUERY PLAN output
        let detail_upper = detail.to_uppercase();

        if detail_upper.starts_with("SCAN TABLE ") {
            // "SCAN TABLE Artist" -> "Artist"
            if let Some(table_name) = detail_upper.strip_prefix("SCAN TABLE ") {
                return Some(table_name.split_whitespace().next()?.to_string());
            }
        } else if detail_upper.starts_with("SCAN ") {
            // "SCAN Artist" -> "Artist" (simpler SQLite format)
            if let Some(table_name) = detail_upper.strip_prefix("SCAN ") {
                return Some(table_name.split_whitespace().next()?.to_string());
            }
        } else if detail_upper.starts_with("SEARCH TABLE ") {
            // "SEARCH TABLE Artist USING INDEX idx_name" -> "Artist"
            if let Some(rest) = detail_upper.strip_prefix("SEARCH TABLE ") {
                return Some(rest.split_whitespace().next()?.to_string());
            }
        } else if detail_upper.starts_with("SEARCH ") {
            // "SEARCH Artist USING INDEX idx_name" -> "Artist" (simpler format)
            if let Some(rest) = detail_upper.strip_prefix("SEARCH ") {
                return Some(rest.split_whitespace().next()?.to_string());
            }
        } else if detail_upper.contains(" TABLE ") {
            // Generic fallback for other patterns
            if let Some(start) = detail_upper.find(" TABLE ") {
                let after_table = &detail_upper[start + 7..]; // 7 = len(" TABLE ")
                return Some(after_table.split_whitespace().next()?.to_string());
            }
        }

        None
    }

    fn calculate_result_hash_from_json(&self, data: &[serde_json::Value]) -> anyhow::Result<u64> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        let json_str = serde_json::to_string(data)?;
        json_str.hash(&mut hasher);
        Ok(hasher.finish())
    }

    fn calculate_result_hash<T: Entity>(&self, data: &[T]) -> anyhow::Result<u64> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();

        // Hash the serialized data for comparison
        for item in data {
            let json_str = serde_json::to_string(item)?;
            json_str.hash(&mut hasher);
        }

        Ok(hasher.finish())
    }

    fn extract_keys_from_json_results(
        &self,
        data: &[serde_json::Value],
    ) -> anyhow::Result<HashSet<String>> {
        let mut keys = HashSet::new();

        for item in data {
            if let Some(obj) = item.as_object() {
                if let Some(key_value) = obj.get("key") {
                    if let Some(key_str) = key_value.as_str() {
                        keys.insert(key_str.to_string());
                    }
                }
            }
        }

        Ok(keys)
    }

    fn extract_keys_from_results<T: Entity>(&self, data: &[T]) -> anyhow::Result<HashSet<String>> {
        let mut keys = HashSet::new();

        for item in data {
            let entity_json = serde_json::to_value(item)?;
            let key = self.extract_key_value_from_json(&entity_json)?;
            keys.insert(key);
        }

        Ok(keys)
    }

    pub fn subscribe_to_query<T: Entity + Send + 'static>(
        &self,
        sql: &str,
        params: &[&dyn rusqlite::ToSql],
    ) -> anyhow::Result<QuerySubscriber<T>> {
        // Generate unique subscription ID
        let subscription_id = Uuid::now_v7().to_string();

        // Extract table dependencies using EXPLAIN QUERY PLAN
        let dependent_tables = self.extract_table_dependencies(sql, params)?;

        // Execute the query to get initial results
        let initial_results: Vec<T> = self.query(sql, params)?;
        let initial_keys = self.extract_keys_from_results(&initial_results)?;
        let initial_hash = self.calculate_result_hash(&initial_results)?;

        // Serialize parameters for storage
        let serialized_params: Vec<String> = params
            .iter()
            .map(|p| self.serialize_tosql_param(p))
            .collect::<Result<Vec<_>, _>>()?;

        // Create subscription
        let subscription = QuerySubscription {
            id: subscription_id.clone(),
            sql: sql.to_string(),
            params: serialized_params,
            dependent_tables,
            last_result_keys: initial_keys.clone(),
            last_result_hash: initial_hash,
        };

        // Store subscription
        {
            let mut subscriptions = self
                .subscriptions
                .write()
                .map_err(|_| anyhow::anyhow!("Failed to acquire write lock on subscriptions"))?;
            subscriptions.insert(subscription_id.clone(), subscription);
        }

        // Create receiver for this subscription
        let rx = self.query_notifier.observer();

        // Send initial result
        let initial_query_result = QueryResult {
            data: initial_results,
            keys: initial_keys,
            hash: initial_hash,
        };

        // Convert to JSON for the generic notifier
        let initial_notification = serde_json::json!({
            "subscription_id": subscription_id,
            "type": "initial",
            "result": serde_json::to_value(&initial_query_result)?
        });

        self.query_notifier.notify(initial_notification);

        // Filter and convert the generic receiver to our specific type
        let (filtered_tx, filtered_rx) = std::sync::mpsc::channel::<QueryResult<T>>();
        let target_subscription_id = subscription_id.clone();

        std::thread::spawn(move || {
            for notification in rx {
                if let Some(obj) = notification.as_object() {
                    if let Some(sub_id) = obj.get("subscription_id").and_then(|v| v.as_str()) {
                        if sub_id == target_subscription_id {
                            if let Some(result_value) = obj.get("result") {
                                if let Ok(query_result) =
                                    serde_json::from_value::<QueryResult<T>>(result_value.clone())
                                {
                                    let _ = filtered_tx.send(query_result);
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(QuerySubscriber {
            subscription_id,
            db: Arc::downgrade(&self.subscriptions),
            _receiver: filtered_rx,
        })
    }

    pub fn observe_query<T: Entity + Send + 'static>(
        &self,
        sql: &str,
        params: &[&dyn rusqlite::ToSql],
        mut callback: impl FnMut(QueryResult<T>) + Send + 'static,
    ) -> anyhow::Result<QueryObserver> {
        // Generate unique subscription ID
        let subscription_id = Uuid::now_v7().to_string();

        // Extract table dependencies using EXPLAIN QUERY PLAN
        let dependent_tables = self.extract_table_dependencies(sql, params)?;

        // Execute the query to get initial results
        let initial_results: Vec<T> = self.query(sql, params)?;
        let initial_keys = self.extract_keys_from_results(&initial_results)?;
        let initial_hash = self.calculate_result_hash(&initial_results)?;

        // Serialize parameters for storage
        let serialized_params: Vec<String> = params
            .iter()
            .map(|p| self.serialize_tosql_param(p))
            .collect::<Result<Vec<_>, _>>()?;

        // Create subscription
        let subscription = QuerySubscription {
            id: subscription_id.clone(),
            sql: sql.to_string(),
            params: serialized_params,
            dependent_tables,
            last_result_keys: initial_keys.clone(),
            last_result_hash: initial_hash,
        };

        // Store subscription
        {
            let mut subscriptions = self
                .subscriptions
                .write()
                .map_err(|_| anyhow::anyhow!("Failed to acquire write lock on subscriptions"))?;
            subscriptions.insert(subscription_id.clone(), subscription);
        }

        // Create initial result and deliver it immediately
        let initial_query_result = QueryResult {
            data: initial_results,
            keys: initial_keys,
            hash: initial_hash,
        };

        // Deliver initial result
        callback(initial_query_result);

        // Set up ongoing observation for future changes
        let target_subscription_id = subscription_id.clone();
        self.query_notifier.observe(move |notification| {
            if let Some(obj) = notification.as_object() {
                if let Some(sub_id) = obj.get("subscription_id").and_then(|v| v.as_str()) {
                    if sub_id == target_subscription_id {
                        if let Some(result_value) = obj.get("result") {
                            if let Ok(query_result) =
                                serde_json::from_value::<QueryResult<T>>(result_value.clone())
                            {
                                callback(query_result);
                            }
                        }
                    }
                }
            }
        });

        // Return cleanup handle
        Ok(QueryObserver {
            subscription_id,
            db: Arc::downgrade(&self.subscriptions),
        })
    }

    fn notify_query_subscribers(
        &self,
        changed_table: &str,
        changed_key: &str,
    ) -> anyhow::Result<()> {
        // Get a snapshot of affected subscriptions
        let affected_subscriptions = {
            let subs = self
                .subscriptions
                .read()
                .map_err(|_| anyhow::anyhow!("Failed to acquire read lock on subscriptions"))?;
            subs.iter()
                .filter(|(_, subscription)| {
                    subscription.dependent_tables.contains(changed_table)
                        || subscription
                            .dependent_tables
                            .contains(&changed_table.to_uppercase())
                        || subscription.last_result_keys.contains(changed_key)
                })
                .map(|(id, sub)| (id.clone(), sub.clone()))
                .collect::<Vec<_>>()
        }; // Lock released here

        // Process affected subscriptions without holding the subscription lock
        for (subscription_id, subscription) in affected_subscriptions {
            // Re-execute the query to check for changes
            self.check_and_notify_subscription(&subscription_id, &subscription)?;
        }

        Ok(())
    }

    fn check_and_notify_subscription(
        &self,
        subscription_id: &str,
        subscription: &QuerySubscription,
    ) -> anyhow::Result<()> {
        // Re-execute the query to get current results
        let params = self.deserialize_params(&subscription.params)?;
        let results: Vec<serde_json::Value> =
            self.query_as_json_with_params(&subscription.sql, &params)?;

        // Calculate new hash and keys
        let new_hash = self.calculate_result_hash_from_json(&results)?;
        let new_keys = self.extract_keys_from_json_results(&results)?;

        // Check if results have changed (compare with subscription state)
        if new_hash != subscription.last_result_hash || new_keys != subscription.last_result_keys {
            // Update subscription with new state - acquire lock briefly
            let mut subscriptions = self.subscriptions.write().map_err(|_| {
                anyhow::anyhow!("Failed to acquire write lock on subscriptions for update")
            })?;
            if let Some(sub) = subscriptions.get_mut(subscription_id) {
                sub.last_result_hash = new_hash;
                sub.last_result_keys = new_keys.clone();
            }
            drop(subscriptions); // Explicitly release lock

            // Create result object (generic JSON for now)
            let query_result = serde_json::json!({
                "data": results,
                "keys": new_keys.into_iter().collect::<Vec<_>>(),
                "hash": new_hash
            });

            // Send notification
            let notification = serde_json::json!({
                "subscription_id": subscription_id,
                "type": "update",
                "result": query_result
            });

            self.query_notifier.notify(notification);
        }

        Ok(())
    }

    fn query_as_json_with_params(
        &self,
        sql: &str,
        params: &[rusqlite::types::Value],
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        let conn = self
            .conn
            .read()
            .map_err(|_| anyhow::anyhow!("Failed to acquire read lock"))?;
        let mut stmt = conn.prepare(sql)?;
        let mut rows = stmt.query(rusqlite::params_from_iter(params))?;

        let mut results = Vec::new();
        while let Some(row) = rows.next()? {
            let column_count = row.as_ref().column_count();
            let mut json_map = serde_json::Map::new();

            for i in 0..column_count {
                let column_name = row.as_ref().column_name(i)?;
                let value: rusqlite::types::Value = row.get(i)?;

                let json_value = match value {
                    rusqlite::types::Value::Null => serde_json::Value::Null,
                    rusqlite::types::Value::Integer(i) => serde_json::Value::Number(i.into()),
                    rusqlite::types::Value::Real(f) => serde_json::Value::Number(
                        serde_json::Number::from_f64(f).unwrap_or(serde_json::Number::from(0)),
                    ),
                    rusqlite::types::Value::Text(s) => serde_json::Value::String(s),
                    rusqlite::types::Value::Blob(_) => {
                        serde_json::Value::String("<binary>".to_string())
                    }
                };

                json_map.insert(column_name.to_string(), json_value);
            }

            results.push(serde_json::Value::Object(json_map));
        }

        Ok(results)
    }

    fn serialize_tosql_param(&self, param: &dyn rusqlite::ToSql) -> anyhow::Result<String> {
        // Convert ToSql parameter to a serializable string representation
        // We use SQLite's value conversion to get a consistent representation
        let conn = self
            .conn
            .read()
            .map_err(|_| anyhow::anyhow!("Failed to acquire read lock"))?;
        let mut stmt = conn.prepare("SELECT ?")?;
        let mut rows = stmt.query([param])?;

        if let Some(row) = rows.next()? {
            let value: rusqlite::types::Value = row.get(0)?;
            Ok(match value {
                rusqlite::types::Value::Null => "NULL".to_string(),
                rusqlite::types::Value::Integer(i) => format!("INT:{}", i),
                rusqlite::types::Value::Real(f) => format!("REAL:{}", f),
                rusqlite::types::Value::Text(s) => format!("TEXT:{}", s),
                rusqlite::types::Value::Blob(_) => {
                    return Err(anyhow::anyhow!("Blob parameters not supported"));
                }
            })
        } else {
            Err(anyhow::anyhow!("Failed to serialize parameter"))
        }
    }

    fn deserialize_params(
        &self,
        serialized_params: &[String],
    ) -> anyhow::Result<Vec<rusqlite::types::Value>> {
        serialized_params
            .iter()
            .map(|s| {
                if s == "NULL" {
                    Ok(rusqlite::types::Value::Null)
                } else if let Some(int_str) = s.strip_prefix("INT:") {
                    Ok(rusqlite::types::Value::Integer(int_str.parse()?))
                } else if let Some(real_str) = s.strip_prefix("REAL:") {
                    Ok(rusqlite::types::Value::Real(real_str.parse()?))
                } else if let Some(text_str) = s.strip_prefix("TEXT:") {
                    Ok(rusqlite::types::Value::Text(text_str.to_string()))
                } else {
                    Err(anyhow::anyhow!("Invalid parameter format: {}", s))
                }
            })
            .collect()
    }

    pub fn get_subscription_count(&self) -> anyhow::Result<usize> {
        let subscriptions = self
            .subscriptions
            .read()
            .map_err(|_| anyhow::anyhow!("Failed to acquire read lock on subscriptions"))?;
        Ok(subscriptions.len())
    }

    pub fn remove_subscription(&self, subscription_id: &str) -> anyhow::Result<bool> {
        let mut subscriptions = self
            .subscriptions
            .write()
            .map_err(|_| anyhow::anyhow!("Failed to acquire write lock on subscriptions"))?;
        Ok(subscriptions.remove(subscription_id).is_some())
    }

    pub fn migrate(&self, migration_sqls: &[&str]) -> anyhow::Result<()> {
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

    pub fn get_metadata(&self, key: &str) -> anyhow::Result<Option<String>> {
        let conn = self
            .conn
            .read()
            .map_err(|_| anyhow::anyhow!("Failed to acquire read lock"))?;

        match conn.query_row("SELECT value FROM _metadata WHERE key = ?", [key], |row| {
            row.get::<_, String>(0)
        }) {
            Ok(value) => Ok(Some(value)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn set_metadata(&self, key: &str, value: &str) -> anyhow::Result<()> {
        let conn = self
            .conn
            .write()
            .map_err(|_| anyhow::anyhow!("Failed to acquire write lock"))?;

        conn.execute(
            "INSERT OR REPLACE INTO _metadata (key, value) VALUES (?, ?)",
            [key, value],
        )?;

        Ok(())
    }

    pub fn get_author(&self) -> &str {
        &self.author
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::{Db, QueryResult};

    #[derive(Serialize, Deserialize, Clone, Default, Debug)]
    pub struct Artist {
        pub key: String,
        pub name: String,
        pub disambiguation: Option<String>,
    }

    // Artist automatically implements Entity due to blanket impl

    #[test]
    fn quick_start() -> anyhow::Result<()> {
        env_logger::init();
        let db = Db::open_memory()?;

        // Demonstrate schema migration: start with basic table, then add disambiguation column
        let migrations = &[
            // Migration 1: Create basic Artist table
            "CREATE TABLE Artist (
                key  TEXT NOT NULL PRIMARY KEY,
                name TEXT NOT NULL
            );",
            // Migration 2: Add disambiguation column
            "ALTER TABLE Artist ADD COLUMN disambiguation TEXT;",
        ];

        db.migrate(migrations)?;

        // Track results from reactive query
        let results = std::sync::Arc::new(std::sync::Mutex::new(Vec::<QueryResult<Artist>>::new()));
        let results_clone = results.clone();

        // Observe reactive query with callback
        let _observer = db.observe_query::<Artist>(
            "SELECT * FROM Artist WHERE name LIKE ?",
            &[&"Metal%"],
            move |result| {
                if let Ok(mut r) = results_clone.lock() {
                    r.push(result);
                }
            },
        )?;

        // Allow time for initial result
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Save artist - triggers reactive notification
        let artist = db.save(&Artist {
            name: "Metallica".to_string(),
            disambiguation: Some("American heavy metal band".to_string()),
            ..Default::default()
        })?;
        assert!(!artist.key.is_empty());
        assert_eq!(
            artist.disambiguation,
            Some("American heavy metal band".to_string())
        );

        // Allow time for reactive update
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Verify we received both initial and updated results
        let r = results.lock().unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].data.len(), 0); // Initial empty result
        assert_eq!(r[1].data.len(), 1); // Updated result with Metallica
        assert_eq!(r[1].data[0].name, "Metallica");
        assert_eq!(
            r[1].data[0].disambiguation,
            Some("American heavy metal band".to_string())
        );

        Ok(())
    }

    #[test]
    fn test_insert_and_update() -> anyhow::Result<()> {
        let db = Db::open_memory()?;
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

        // Test INSERT (new record)
        let artist1 = db.save(&Artist {
            name: "Iron Maiden".to_string(),
            disambiguation: Some("British metal band".to_string()),
            ..Default::default()
        })?;

        // Test UPDATE (existing record)
        let artist2 = db.save(&Artist {
            key: artist1.key.clone(),
            name: "Iron Maiden".to_string(),
            disambiguation: Some("British heavy metal band".to_string()),
        })?;

        // Verify the key is the same but disambiguation was updated
        assert_eq!(artist1.key, artist2.key);
        assert_eq!(
            artist2.disambiguation,
            Some("British heavy metal band".to_string())
        );

        Ok(())
    }

    #[derive(Serialize, Deserialize, Clone, Default, Debug)]
    pub struct ArtistWithExtra {
        pub key: String,
        pub name: String,
        pub disambiguation: Option<String>,
        pub extra_field: Option<String>, // This field doesn't exist in the Artist table
    }

    #[test]
    fn test_save_with_extra_fields() -> anyhow::Result<()> {
        let db = Db::open_memory()?;
        if let Ok(conn) = db.conn.write() {
            conn.execute_batch(
                "
                CREATE TABLE ArtistWithExtra (
                    key            TEXT NOT NULL PRIMARY KEY,
                    name           TEXT NOT NULL,
                    disambiguation TEXT
                    -- Note: extra_field column is intentionally missing
                );
            ",
            )?;
        }

        // Test that saving works even when struct has fields not in the database table
        let artist = db.save(&ArtistWithExtra {
            name: "Pink Floyd".to_string(),
            disambiguation: Some("Progressive rock band".to_string()),
            extra_field: Some("This field should be ignored".to_string()),
            ..Default::default()
        })?;

        // Verify the saved entity still has all original fields
        assert_eq!(artist.name, "Pink Floyd");
        assert_eq!(
            artist.disambiguation,
            Some("Progressive rock band".to_string())
        );
        assert_eq!(
            artist.extra_field,
            Some("This field should be ignored".to_string())
        );
        assert!(!artist.key.is_empty());

        // Test UPDATE with extra fields
        let updated_artist = db.save(&ArtistWithExtra {
            key: artist.key.clone(),
            name: "Pink Floyd".to_string(),
            disambiguation: Some("English progressive rock band".to_string()),
            extra_field: Some("Still ignored".to_string()),
        })?;

        // Verify UPDATE worked and key remained the same
        assert_eq!(artist.key, updated_artist.key);
        assert_eq!(
            updated_artist.disambiguation,
            Some("English progressive rock band".to_string())
        );
        assert_eq!(
            updated_artist.extra_field,
            Some("Still ignored".to_string())
        );

        Ok(())
    }

    #[test]
    fn test_transaction_rollback_on_nonexistent_table() -> anyhow::Result<()> {
        let db = Db::open_memory()?;

        // Don't create any table - this should cause the save to fail

        #[derive(Serialize, Deserialize, Clone, Default, Debug)]
        pub struct NonExistentTable {
            pub key: String,
            pub name: String,
        }

        // Try to save to a non-existent table - this should fail and rollback
        let result = db.save(&NonExistentTable {
            name: "Test".to_string(),
            ..Default::default()
        });

        // Verify that the operation failed
        assert!(result.is_err());

        // The error should be about the table not being found
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("not found") || error_msg.contains("no such table"));

        Ok(())
    }

    #[test]
    fn test_query() -> anyhow::Result<()> {
        let db = Db::open_memory()?;
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

        // Insert some test data
        let artist1 = db.save(&Artist {
            name: "Metallica".to_string(),
            disambiguation: Some("American metal band".to_string()),
            ..Default::default()
        })?;

        let _artist2 = db.save(&Artist {
            name: "Iron Maiden".to_string(),
            disambiguation: Some("British metal band".to_string()),
            ..Default::default()
        })?;

        // Test query all
        let all_artists: Vec<Artist> = db.query("SELECT * FROM Artist ORDER BY name", &[])?;
        assert_eq!(all_artists.len(), 2);
        assert_eq!(all_artists[0].name, "Iron Maiden");
        assert_eq!(all_artists[1].name, "Metallica");

        // Test query with parameters
        let filtered_artists: Vec<Artist> =
            db.query("SELECT * FROM Artist WHERE name = ?", &[&"Metallica"])?;
        assert_eq!(filtered_artists.len(), 1);
        assert_eq!(filtered_artists[0].name, "Metallica");
        assert_eq!(filtered_artists[0].key, artist1.key);

        // Test query with no results
        let no_artists: Vec<Artist> =
            db.query("SELECT * FROM Artist WHERE name = ?", &[&"NonExistent"])?;
        assert_eq!(no_artists.len(), 0);

        Ok(())
    }

    #[test]
    fn test_transaction_atomicity() -> anyhow::Result<()> {
        let db = Db::open_memory()?;
        if let Ok(conn) = db.conn.write() {
            conn.execute_batch(
                "
                CREATE TABLE TestAtomicity (
                    key            TEXT NOT NULL PRIMARY KEY,
                    name           TEXT NOT NULL,
                    required_field TEXT NOT NULL  -- This will cause constraint violation
                );
            ",
            )?;
        }

        #[derive(Serialize, Deserialize, Clone, Default, Debug)]
        pub struct TestAtomicity {
            pub key: String,
            pub name: String,
            // Note: missing required_field - this will cause the save to fail
        }

        // First, successfully insert a record
        if let Ok(conn) = db.conn.write() {
            conn.execute("INSERT INTO TestAtomicity (key, name, required_field) VALUES ('test1', 'Test 1', 'value')", [])?;
        }

        // Verify the record exists
        if let Ok(conn) = db.conn.write() {
            let count: i64 =
                conn.query_row("SELECT COUNT(*) FROM TestAtomicity", [], |row| row.get(0))?;
            assert_eq!(count, 1);
        }

        // Now try to save using our method - this should fail due to missing required_field
        // and the transaction should rollback, leaving the original record intact
        let result = db.save(&TestAtomicity {
            name: "Test 2".to_string(),
            ..Default::default()
        });

        // The save should fail
        assert!(result.is_err());

        // Verify that the original record is still there and no new record was added
        if let Ok(conn) = db.conn.write() {
            let count: i64 =
                conn.query_row("SELECT COUNT(*) FROM TestAtomicity", [], |row| row.get(0))?;
            assert_eq!(count, 1); // Should still be 1, not 2

            // Verify the original record is unchanged
            let name: String = conn.query_row(
                "SELECT name FROM TestAtomicity WHERE key = 'test1'",
                [],
                |row| row.get(0),
            )?;
            assert_eq!(name, "Test 1");
        }

        Ok(())
    }

    #[test]
    fn test_change_tracking_insert() -> anyhow::Result<()> {
        let db = crate::Db::open_memory()?;

        // Create test table
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

        // Save a new artist - should create Insert change
        let artist = db.save(&Artist {
            name: "Metallica".to_string(),
            disambiguation: Some("American metal band".to_string()),
            ..Default::default()
        })?;

        // Query changes
        let changes = db.get_changes_for_entity("Artist", &artist.key)?;
        assert_eq!(changes.len(), 1);

        let change = &changes[0];
        assert_eq!(change.entity_type, "Artist");
        assert_eq!(change.entity_key, artist.key);
        assert_eq!(change.change_type, "Insert");
        assert!(change.old_values.is_none());
        assert!(change.new_values.is_some());

        Ok(())
    }

    #[test]
    fn test_change_tracking_update() -> anyhow::Result<()> {
        let db = crate::Db::open_memory()?;

        // Create test table
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

        // Save a new artist
        let artist1 = db.save(&Artist {
            name: "Iron Maiden".to_string(),
            disambiguation: Some("British metal band".to_string()),
            ..Default::default()
        })?;

        // Update the artist - should create Update change
        let _artist2 = db.save(&Artist {
            key: artist1.key.clone(),
            name: "Iron Maiden".to_string(),
            disambiguation: Some("British heavy metal band".to_string()),
        })?;

        // Query changes for this entity
        let changes = db.get_changes_for_entity("Artist", &artist1.key)?;
        assert_eq!(changes.len(), 2); // Insert + Update

        // Check Insert change
        let insert_change = &changes[0];
        assert_eq!(insert_change.change_type, "Insert");
        assert!(insert_change.old_values.is_none());

        // Check Update change
        let update_change = &changes[1];
        assert_eq!(update_change.change_type, "Update");
        assert!(update_change.old_values.is_some());
        assert!(update_change.new_values.is_some());

        Ok(())
    }

    #[test]
    fn test_database_uuid_persistence() -> anyhow::Result<()> {
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
        assert!(!db.author.is_empty());
        assert!(uuid::Uuid::parse_str(&db.author).is_ok());

        Ok(())
    }

    #[test]
    fn test_get_changes_since_timestamp() -> anyhow::Result<()> {
        let db = crate::Db::open_memory()?;

        // Create test table
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

        let start_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as i64
            - 100; // Subtract 100ms to account for timing

        // Save two artists
        let _artist1 = db.save(&Artist {
            name: "Metallica".to_string(),
            ..Default::default()
        })?;

        let _artist2 = db.save(&Artist {
            name: "Iron Maiden".to_string(),
            ..Default::default()
        })?;

        // Get changes since start time
        let changes = db.get_changes_since(start_time)?;
        assert_eq!(changes.len(), 2);

        // Get changes since now (should be empty)
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as i64;
        let recent_changes = db.get_changes_since(now)?;
        assert_eq!(recent_changes.len(), 0);

        Ok(())
    }

    #[test]
    fn test_get_changes_by_author() -> anyhow::Result<()> {
        let db = crate::Db::open_memory()?;

        // Create test table
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

        // Save an artist
        let _artist = db.save(&Artist {
            name: "Metallica".to_string(),
            ..Default::default()
        })?;

        // Get changes by correct author (using the actual db author UUID)
        let changes = db.get_changes_by_author(&db.author)?;
        assert_eq!(changes.len(), 1);

        // Get changes by different author (should be empty)
        let other_changes = db.get_changes_by_author("other-author")?;
        assert_eq!(other_changes.len(), 0);

        Ok(())
    }

    #[test]
    fn test_reactive_query_basic() -> anyhow::Result<()> {
        let db = crate::Db::open_memory()?;

        // Create test table
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

        // Subscribe to a query
        let subscriber =
            db.subscribe_to_query::<Artist>("SELECT * FROM Artist WHERE name LIKE 'Metal%'", &[])?;

        // Collect initial result
        let initial_result = subscriber.recv_timeout(std::time::Duration::from_millis(100))?;
        assert_eq!(initial_result.data.len(), 0); // No matching artists initially

        // Save an artist that should trigger the query
        let _artist = db.save(&Artist {
            name: "Metallica".to_string(),
            disambiguation: Some("American metal band".to_string()),
            ..Default::default()
        })?;

        // Should receive a notification
        let updated_result = subscriber.recv_timeout(std::time::Duration::from_millis(1000))?;
        assert_eq!(updated_result.data.len(), 1);
        assert_eq!(updated_result.data[0].name, "Metallica");

        Ok(())
    }

    #[test]
    fn test_reactive_query_no_match() -> anyhow::Result<()> {
        let db = crate::Db::open_memory()?;

        // Create test table
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

        // Subscribe to a specific query
        let subscriber =
            db.subscribe_to_query::<Artist>("SELECT * FROM Artist WHERE name = 'Metallica'", &[])?;

        // Collect initial result
        let initial_result = subscriber.recv_timeout(std::time::Duration::from_millis(100))?;
        assert_eq!(initial_result.data.len(), 0);

        // Save an artist that should trigger re-evaluation but result in same empty set
        let _artist = db.save(&Artist {
            name: "Iron Maiden".to_string(),
            disambiguation: Some("British metal band".to_string()),
            ..Default::default()
        })?;

        // Should receive a notification (table changed) but result set should still be empty
        let updated_result = subscriber.recv_timeout(std::time::Duration::from_millis(1000))?;
        assert_eq!(updated_result.data.len(), 0); // Still no matching artists

        Ok(())
    }

    #[test]
    fn test_explain_query_plan_parsing() -> anyhow::Result<()> {
        let db = crate::Db::open_memory()?;

        // Create test table
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

        // Test table dependency extraction
        let tables = db.extract_table_dependencies("SELECT * FROM Artist", &[])?;
        assert!(tables.contains("ARTIST")); // SQLite normalizes to uppercase

        Ok(())
    }

    #[test]
    fn test_subscription_management() -> anyhow::Result<()> {
        let db = crate::Db::open_memory()?;

        // Create test table
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

        // Initially no subscriptions
        assert_eq!(db.get_subscription_count()?, 0);

        // Subscribe to a query
        let _subscriber = db.subscribe_to_query::<Artist>("SELECT * FROM Artist", &[])?;

        // Should have 1 subscription
        assert_eq!(db.get_subscription_count()?, 1);

        Ok(())
    }

    #[test]
    fn test_observe_query_callback() -> anyhow::Result<()> {
        let db = crate::Db::open_memory()?;

        // Create test table
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

        // Use Arc<Mutex<Vec>> to collect results from callback
        let results = std::sync::Arc::new(std::sync::Mutex::new(Vec::<QueryResult<Artist>>::new()));
        let results_clone = results.clone();

        // Observe query with callback
        let _observer = db.observe_query::<Artist>(
            "SELECT * FROM Artist WHERE name LIKE ?",
            &[&"Metal%"],
            move |query_result| {
                if let Ok(mut r) = results_clone.lock() {
                    r.push(query_result);
                }
            },
        )?;

        // Give callback time to process initial result
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Should have received initial empty result
        {
            let r = results.lock().unwrap();
            assert_eq!(r.len(), 1);
            assert_eq!(r[0].data.len(), 0);
        }

        // Save an artist that matches the query
        let _artist = db.save(&Artist {
            name: "Metallica".to_string(),
            disambiguation: Some("American metal band".to_string()),
            ..Default::default()
        })?;

        // Give callback time to process the update
        std::thread::sleep(std::time::Duration::from_millis(50));

        // Should have received both initial and updated results
        {
            let r = results.lock().unwrap();
            assert_eq!(r.len(), 2);
            assert_eq!(r[0].data.len(), 0); // Initial empty result
            assert_eq!(r[1].data.len(), 1); // Updated result with Metallica
            assert_eq!(r[1].data[0].name, "Metallica");
        }

        Ok(())
    }

    #[test]
    fn test_subscription_cleanup_on_drop() -> anyhow::Result<()> {
        let db = crate::Db::open_memory()?;

        // Create test table
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

        // Initially no subscriptions
        assert_eq!(db.get_subscription_count()?, 0);

        // Create subscription in inner scope
        {
            let _subscriber = db.subscribe_to_query::<Artist>("SELECT * FROM Artist", &[])?;
            // Should have 1 subscription
            assert_eq!(db.get_subscription_count()?, 1);
        } // subscriber drops here

        // Give cleanup time to happen
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Should be cleaned up
        assert_eq!(db.get_subscription_count()?, 0);

        Ok(())
    }

    #[test]
    fn test_observer_cleanup_on_drop() -> anyhow::Result<()> {
        let db = crate::Db::open_memory()?;

        // Create test table
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

        // Initially no subscriptions
        assert_eq!(db.get_subscription_count()?, 0);

        // Create observer in inner scope
        {
            let _observer = db.observe_query::<Artist>(
                "SELECT * FROM Artist WHERE name LIKE ?",
                &[&"Metal%"],
                |_| {},
            )?;
            // Should have 1 subscription
            assert_eq!(db.get_subscription_count()?, 1);
        } // observer drops here

        // Give cleanup time to happen
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Should be cleaned up
        assert_eq!(db.get_subscription_count()?, 0);

        Ok(())
    }

    #[test]
    fn test_save_performance_comparison() -> anyhow::Result<()> {
        let db = crate::Db::open_memory()?;

        // Create test table
        if let Ok(conn) = db.conn.write() {
            conn.execute_batch(
                "
                CREATE TABLE Artist (
                    key            TEXT NOT NULL PRIMARY KEY,
                    name           TEXT NOT NULL,
                    disambiguation TEXT
                );
                
                CREATE TABLE Artist_Raw (
                    key            TEXT NOT NULL PRIMARY KEY,
                    name           TEXT NOT NULL,
                    disambiguation TEXT
                );
            ",
            )?;
        }

        let artist = Artist {
            name: "Test Artist".to_string(),
            disambiguation: Some("Test Band".to_string()),
            ..Default::default()
        };

        let iterations = 1000;

        // Test our optimized save() function
        let start = std::time::Instant::now();
        for i in 0..iterations {
            let mut test_artist = artist.clone();
            test_artist.name = format!("Test Artist {}", i);
            db.save(&test_artist)?;
        }
        let optimized_duration = start.elapsed();

        // Test raw SQLite insert (no change tracking, no transactions)
        let start = std::time::Instant::now();
        {
            let conn = db.conn.write().unwrap();
            for i in 0..iterations {
                let key = uuid::Uuid::now_v7().to_string();
                let name = format!("Raw Artist {}", i);
                conn.execute(
                    "INSERT INTO Artist_Raw (key, name, disambiguation) VALUES (?, ?, ?)",
                    rusqlite::params![key, name, "Raw Band"],
                )?;
            }
        }
        let raw_duration = start.elapsed();

        println!("Optimized save(): {:?}", optimized_duration);
        println!("Raw SQLite insert: {:?}", raw_duration);
        println!(
            "Overhead ratio: {:.2}x",
            optimized_duration.as_secs_f64() / raw_duration.as_secs_f64()
        );

        // Verify we have the expected number of records
        let optimized_count: i64 = {
            let conn = db.conn.read().unwrap();
            conn.query_row("SELECT COUNT(*) FROM Artist", [], |row| row.get(0))?
        };
        let raw_count: i64 = {
            let conn = db.conn.read().unwrap();
            conn.query_row("SELECT COUNT(*) FROM Artist_Raw", [], |row| row.get(0))?
        };

        assert_eq!(optimized_count, iterations as i64);
        assert_eq!(raw_count, iterations as i64);

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
        db.migrate(migrations)?;

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
        db.migrate(migrations)?;
        db.migrate(migrations)?;

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
