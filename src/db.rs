use std::sync::{Arc, RwLock};

use rusqlite::Connection;
use serde::{de::DeserializeOwned, Serialize};
use uuid::Uuid;

pub trait Entity: Serialize + DeserializeOwned {}

// Blanket implementation for any type that meets the requirements
impl<T> Entity for T where T: Serialize + DeserializeOwned {}

#[derive(Clone)]
pub struct Db {
    conn: Arc<RwLock<Connection>>,
}

impl Db {
    pub fn open_memory() -> anyhow::Result<Self> {
        let conn = Arc::new(RwLock::new(Connection::open_in_memory()?));
        let db = Db { conn };
        db.init_connection()?;
        Ok(db)
    }

    fn init_connection(&self) -> anyhow::Result<()> {
        if let Ok(conn) = self.conn.write() {
            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "foreign_keys", "ON")?;
        }
        Ok(())
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
        
        if exists {
            self.update_record(&tx, &table_name, &key_value, all_params, &table_columns)?;
        } else {
            self.insert_record(&tx, &table_name, all_params, &table_columns)?;
        }
        
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

}

