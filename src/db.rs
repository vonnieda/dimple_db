use std::sync::{Arc, RwLock};

use rusqlite::Connection;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use uuid::Uuid;

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

#[derive(Clone)]
pub struct Db {
    conn: Arc<RwLock<Connection>>,
    author: String,
}

impl Db {
    pub fn open_memory() -> anyhow::Result<Self> {
        let conn = Arc::new(RwLock::new(Connection::open_in_memory()?));
        let db = Db { 
            conn,
            author: "".to_string(), // Will be set after initialization
        };
        db.init_connection()?;
        db.init_change_tracking_tables()?;
        let author = db.get_or_create_database_uuid()?;
        let mut db = db;
        db.author = author;
        Ok(db)
    }

    fn init_connection(&self) -> anyhow::Result<()> {
        if let Ok(conn) = self.conn.write() {
            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "foreign_keys", "ON")?;
        }
        Ok(())
    }

    fn init_change_tracking_tables(&self) -> anyhow::Result<()> {
        if let Ok(conn) = self.conn.write() {
            conn.execute_batch("
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
            ")?;
        }
        Ok(())
    }

    fn get_or_create_database_uuid(&self) -> anyhow::Result<String> {
        let conn = self.conn.read().map_err(|_| anyhow::anyhow!("Failed to acquire read lock"))?;
        
        // Try to get existing UUID
        let existing_uuid = conn.query_row(
            "SELECT value FROM _metadata WHERE key = 'database_uuid'",
            [],
            |row| row.get::<_, String>(0)
        );
        
        match existing_uuid {
            Ok(uuid) => Ok(uuid),
            Err(_) => {
                // Create new UUID and store it
                drop(conn); // Release read lock
                let conn = self.conn.write().map_err(|_| anyhow::anyhow!("Failed to acquire write lock"))?;
                let new_uuid = Uuid::now_v7().to_string();
                
                conn.execute(
                    "INSERT INTO _metadata (key, value) VALUES ('database_uuid', ?)",
                    [&new_uuid]
                )?;
                
                Ok(new_uuid)
            }
        }
    }
    
    pub fn query<T: Entity>(&self, sql: &str, params: &[&dyn rusqlite::ToSql]) -> anyhow::Result<Vec<T>> {
        let conn = self.conn.read().map_err(|_| anyhow::anyhow!("Failed to acquire read lock"))?;
        
        let mut stmt = conn.prepare(sql)?;
        let mut rows = stmt.query(params)?;
        
        let mut results = Vec::new();
        while let Some(row) = rows.next()? {
            let entity = serde_rusqlite::from_row::<T>(row)?;
            results.push(entity);
        }
        
        Ok(results)
    }

    pub fn save<T: Entity>(&self, entity: &T) -> anyhow::Result<T> {
        let table_name = self.struct_name(entity)?;
        let final_entity = self.ensure_entity_has_key(entity)?;
        
        let mut conn = self.conn.write().map_err(|_| anyhow::anyhow!("Failed to acquire write lock"))?;
        let tx = conn.transaction()?;
        
        let table_columns = self.get_table_columns(&tx, &table_name)?;
        let all_params = serde_rusqlite::to_params_named(&final_entity)?;
        
        let key_value = self.extract_key_value(&final_entity)?;
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
        let new_values = serde_json::to_string(&final_entity)?;
        self.record_change(&tx, &table_name, &key_value, change_type, old_values, Some(new_values))?;
        
        tx.commit()?;
        
        Ok(final_entity)
    }
    
    
    fn ensure_entity_has_key<T: Entity>(&self, entity: &T) -> anyhow::Result<T> {
        let mut entity_json = serde_json::to_value(entity)?;
        
        // Generate a key if it doesn't exist or is empty
        let needs_key = match entity_json.get("key") {
            Some(key) => key.is_null() || (key.is_string() && key.as_str() == Some("")),
            None => true,
        };
        
        if needs_key {
            entity_json["key"] = serde_json::Value::String(Uuid::now_v7().to_string());
        }
        
        Ok(serde_json::from_value(entity_json)?)
    }
    
    
    fn extract_key_value<T: Entity>(&self, entity: &T) -> anyhow::Result<String> {
        let entity_json = serde_json::to_value(entity)?;
        entity_json
            .get("key")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("Entity missing required 'key' field"))
    }
    
    fn record_exists(&self, tx: &rusqlite::Transaction, table_name: &str, key: &str) -> anyhow::Result<bool> {
        let sql = format!("SELECT 1 FROM {} WHERE key = ? LIMIT 1", table_name);
        Ok(tx.prepare(&sql)?.exists([key])?)
    }
    
    fn update_record(
        &self,
        tx: &rusqlite::Transaction,
        table_name: &str,
        key: &str,
        all_params: serde_rusqlite::NamedParamSlice,
        table_columns: &[String]
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
        
        let sql = format!("UPDATE {} SET {} WHERE key = ?", table_name, set_clauses.join(", "));
        let mut stmt = tx.prepare(&sql)?;
        
        // Build parameter list: all non-key values + key for WHERE clause
        let mut values: Vec<&dyn rusqlite::ToSql> = filtered_params
            .iter()
            .map(|(_, value)| *value)
            .collect();
        values.push(&key);
        
        stmt.execute(&values[..])?;
        Ok(())
    }
    
    fn insert_record(
        &self,
        tx: &rusqlite::Transaction,
        table_name: &str,
        all_params: serde_rusqlite::NamedParamSlice,
        table_columns: &[String]
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
            return Err(anyhow::anyhow!("No valid columns found for table '{}'", table_name));
        }
        
        let column_names: Vec<&str> = filtered_params.iter().map(|(name, _)| name.as_str()).collect();
        let placeholders = vec!["?"; filtered_params.len()].join(", ");
        
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})", 
            table_name, 
            column_names.join(", "),
            placeholders
        );
        
        let mut stmt = tx.prepare(&sql)?;
        let values: Vec<&dyn rusqlite::ToSql> = filtered_params.iter().map(|(_, value)| *value).collect();
        stmt.execute(&values[..])?;
        
        Ok(())
    }
    
    fn get_table_columns(&self, conn: &rusqlite::Connection, table_name: &str) -> anyhow::Result<Vec<String>> {
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
            return Err(anyhow::anyhow!("Table '{}' not found or has no columns", table_name));
        }
        
        Ok(columns)
    }

    pub fn struct_name<T>(&self, _value: &T) -> anyhow::Result<String> {
        let full_name = std::any::type_name::<T>();
        
        // Extract just the struct name from the full path
        // e.g. "dimple_data::db::tests::Artist" -> "Artist"
        let name = full_name
            .split("::")
            .last()
            .unwrap_or(full_name);
        
        Ok(name.to_string())
    }

    fn get_record_as_json(&self, tx: &rusqlite::Transaction, table_name: &str, key: &str) -> anyhow::Result<String> {
        let sql = format!("SELECT * FROM {} WHERE key = ?", table_name);
        let mut stmt = tx.prepare(&sql)?;
        let mut rows = stmt.query([key])?;
        
        if let Some(row) = rows.next()? {
            // Convert SQLite row to JSON
            let column_count = row.as_ref().column_count();
            let mut json_map = serde_json::Map::new();
            
            for i in 0..column_count {
                let column_name = row.as_ref().column_name(i)?;
                let value: rusqlite::types::Value = row.get(i)?;
                
                let json_value = match value {
                    rusqlite::types::Value::Null => serde_json::Value::Null,
                    rusqlite::types::Value::Integer(i) => serde_json::Value::Number(i.into()),
                    rusqlite::types::Value::Real(f) => serde_json::Value::Number(
                        serde_json::Number::from_f64(f).unwrap_or(serde_json::Number::from(0))
                    ),
                    rusqlite::types::Value::Text(s) => serde_json::Value::String(s),
                    // TODO need to look closer at this
                    rusqlite::types::Value::Blob(_) => serde_json::Value::String("<binary>".to_string()),
                };
                
                json_map.insert(column_name.to_string(), json_value);
            }
            
            Ok(serde_json::to_string(&json_map)?)
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
        
        tx.execute(
            "INSERT INTO _transaction (id, timestamp, author, bundle_id) VALUES (?, ?, ?, ?)",
            rusqlite::params![transaction_id, timestamp, self.author, Option::<String>::None],
        )?;
        
        // Create change record
        let change_id = Uuid::now_v7().to_string();
        let change_type_str = match change_type {
            ChangeType::Insert => "Insert",
            ChangeType::Update => "Update", 
            ChangeType::Delete => "Delete",
        };
        
        tx.execute(
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

    pub fn get_changes_for_entity(&self, entity_type: &str, entity_key: &str) -> anyhow::Result<Vec<Change>> {
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

    pub fn get_transactions_since(&self, timestamp: i64) -> anyhow::Result<Vec<Transaction>> {
        self.query(
            "SELECT id, timestamp, author, bundle_id 
             FROM _transaction 
             WHERE timestamp > ? 
             ORDER BY timestamp ASC",
            &[&timestamp],
        )
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
    
    // Artist automatically implements Entity due to blanket impl

    #[test]
    fn quick_start() -> anyhow::Result<()> {
        env_logger::init();
        let db = Db::open_memory()?;
        if let Ok(conn) = db.conn.write() {
            conn.execute_batch("
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

    #[test]
    fn test_insert_and_update() -> anyhow::Result<()> {
        let db = Db::open_memory()?;
        if let Ok(conn) = db.conn.write() {
            conn.execute_batch("
                CREATE TABLE Artist (
                    key            TEXT NOT NULL PRIMARY KEY,
                    name           TEXT NOT NULL,
                    disambiguation TEXT
                );
            ")?;
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
        assert_eq!(artist2.disambiguation, Some("British heavy metal band".to_string()));
        
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
            conn.execute_batch("
                CREATE TABLE ArtistWithExtra (
                    key            TEXT NOT NULL PRIMARY KEY,
                    name           TEXT NOT NULL,
                    disambiguation TEXT
                    -- Note: extra_field column is intentionally missing
                );
            ")?;
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
        assert_eq!(artist.disambiguation, Some("Progressive rock band".to_string()));
        assert_eq!(artist.extra_field, Some("This field should be ignored".to_string()));
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
        assert_eq!(updated_artist.disambiguation, Some("English progressive rock band".to_string()));
        assert_eq!(updated_artist.extra_field, Some("Still ignored".to_string()));
        
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
            conn.execute_batch("
                CREATE TABLE Artist (
                    key            TEXT NOT NULL PRIMARY KEY,
                    name           TEXT NOT NULL,
                    disambiguation TEXT
                );
            ")?;
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
        let filtered_artists: Vec<Artist> = db.query(
            "SELECT * FROM Artist WHERE name = ?", 
            &[&"Metallica"]
        )?;
        assert_eq!(filtered_artists.len(), 1);
        assert_eq!(filtered_artists[0].name, "Metallica");
        assert_eq!(filtered_artists[0].key, artist1.key);
        
        // Test query with no results
        let no_artists: Vec<Artist> = db.query(
            "SELECT * FROM Artist WHERE name = ?", 
            &[&"NonExistent"]
        )?;
        assert_eq!(no_artists.len(), 0);
        
        Ok(())
    }

    #[test] 
    fn test_transaction_atomicity() -> anyhow::Result<()> {
        let db = Db::open_memory()?;
        if let Ok(conn) = db.conn.write() {
            conn.execute_batch("
                CREATE TABLE TestAtomicity (
                    key            TEXT NOT NULL PRIMARY KEY,
                    name           TEXT NOT NULL,
                    required_field TEXT NOT NULL  -- This will cause constraint violation
                );
            ")?;
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
            let count: i64 = conn.query_row("SELECT COUNT(*) FROM TestAtomicity", [], |row| row.get(0))?;
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
            let count: i64 = conn.query_row("SELECT COUNT(*) FROM TestAtomicity", [], |row| row.get(0))?;
            assert_eq!(count, 1); // Should still be 1, not 2
            
            // Verify the original record is unchanged
            let name: String = conn.query_row("SELECT name FROM TestAtomicity WHERE key = 'test1'", [], |row| row.get(0))?;
            assert_eq!(name, "Test 1");
        }
        
        Ok(())
    }

    #[test]
    fn test_change_tracking_insert() -> anyhow::Result<()> {
        let db = crate::Db::open_memory()?;
        
        // Create test table
        if let Ok(conn) = db.conn.write() {
            conn.execute_batch("
                CREATE TABLE Artist (
                    key            TEXT NOT NULL PRIMARY KEY,
                    name           TEXT NOT NULL,
                    disambiguation TEXT
                );
            ")?;
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
            conn.execute_batch("
                CREATE TABLE Artist (
                    key            TEXT NOT NULL PRIMARY KEY,
                    name           TEXT NOT NULL,
                    disambiguation TEXT
                );
            ")?;
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
            conn.execute_batch("
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
            conn.execute_batch("
                CREATE TABLE Artist (
                    key            TEXT NOT NULL PRIMARY KEY,
                    name           TEXT NOT NULL,
                    disambiguation TEXT
                );
            ")?;
        }
        
        let start_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as i64 - 100; // Subtract 100ms to account for timing
        
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
            conn.execute_batch("
                CREATE TABLE Artist (
                    key            TEXT NOT NULL PRIMARY KEY,
                    name           TEXT NOT NULL,
                    disambiguation TEXT
                );
            ")?;
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

}

