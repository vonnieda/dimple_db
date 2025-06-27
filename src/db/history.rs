use serde_json::Value;
use uuid::Uuid;

use super::core::Db;
use super::types::{Change, Transaction};

impl Db {
    pub(crate) fn record_changes(
        &self,
        tx: &rusqlite::Transaction,
        entity_type: &str,
        entity_key: &str,
        old_entity: Option<&Value>,
        new_entity: Option<&Value>,
    ) -> anyhow::Result<()> {
        let changes = Self::diff_entities(old_entity, new_entity);
        
        if changes.is_empty() {
            return Ok(()); // No changes to record
        }

        // Create transaction record
        let transaction_id = Uuid::now_v7().to_string();
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as i64;

        log::debug!(
            "SQL EXECUTE: INSERT INTO _transaction (id, timestamp, author) VALUES (?, ?, ?)"
        );
        let tx_affected = tx.execute(
            "INSERT INTO _transaction (id, timestamp, author) VALUES (?, ?, ?)",
            rusqlite::params![
                transaction_id,
                timestamp,
                self.database_uuid,
            ],
        )?;
        log::debug!("SQL EXECUTE RESULT: {} rows affected", tx_affected);

        // Insert change records for each attribute
        for (attribute, old_value, new_value) in changes {
            log::debug!(
                "SQL EXECUTE: INSERT INTO _change (transaction_id, entity_type, entity_key, attribute, old_value, new_value) VALUES (?, ?, ?, ?, ?, ?)"
            );
            let ch_affected = tx.execute(
                "INSERT INTO _change (transaction_id, entity_type, entity_key, attribute, old_value, new_value) VALUES (?, ?, ?, ?, ?, ?)",
                rusqlite::params![
                    transaction_id,
                    entity_type,
                    entity_key,
                    attribute,
                    old_value,
                    new_value,
                ],
            )?;
            log::debug!("SQL EXECUTE RESULT: {} rows affected", ch_affected);
        }

        Ok(())
    }

    fn diff_entities(
        old_entity: Option<&Value>,
        new_entity: Option<&Value>,
    ) -> Vec<(String, Option<String>, Option<String>)> {
        let mut changes = Vec::new();
        
        match (old_entity, new_entity) {
            (None, None) => {}, // No change
            (None, Some(new)) => {
                // Entity creation - record non-null attributes
                if let Some(obj) = new.as_object() {
                    for (key, value) in obj {
                        // Skip null values during entity creation
                        if !value.is_null() {
                            changes.push((
                                key.clone(),
                                None,
                                Some(value.to_string()),
                            ));
                        }
                    }
                }
            },
            (Some(_old), None) => {
                // Entity deletion - all attributes are removed
                if let Some(obj) = old_entity.unwrap().as_object() {
                    for (key, value) in obj {
                        changes.push((
                            key.clone(),
                            Some(value.to_string()),
                            None,
                        ));
                    }
                }
            },
            (Some(old), Some(new)) => {
                // Entity update - find changed attributes
                let empty_map = serde_json::Map::new();
                let old_obj = old.as_object().unwrap_or(&empty_map);
                let new_obj = new.as_object().unwrap_or(&empty_map);
                
                // Find all attributes in either old or new
                let mut all_keys = std::collections::HashSet::new();
                all_keys.extend(old_obj.keys());
                all_keys.extend(new_obj.keys());
                
                for key in all_keys {
                    let old_val = old_obj.get(key);
                    let new_val = new_obj.get(key);
                    
                    if old_val != new_val {
                        changes.push((
                            key.clone(),
                            old_val.map(|v| v.to_string()),
                            new_val.map(|v| v.to_string()),
                        ));
                    }
                }
            },
        }
        
        changes
    }

    pub fn get_changes_since_uuid(&self, since_uuid: &str) -> anyhow::Result<Vec<Change>> {
        self.query(
            "SELECT c.transaction_id, c.entity_type, c.entity_key, c.attribute, c.old_value, c.new_value
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
            "SELECT c.transaction_id, c.entity_type, c.entity_key, c.attribute, c.old_value, c.new_value
             FROM _change c 
             JOIN _transaction t ON c.transaction_id = t.id 
             WHERE c.entity_type = ? AND c.entity_key = ? 
             ORDER BY t.timestamp ASC",
            &[&entity_type, &entity_key],
        )
    }

    pub fn get_all_changes(&self) -> anyhow::Result<Vec<Change>> {
        self.query(
            "SELECT c.transaction_id, c.entity_type, c.entity_key, c.attribute, c.old_value, c.new_value
             FROM _change c 
             JOIN _transaction t ON c.transaction_id = t.id 
             ORDER BY t.timestamp ASC",
            &[],
        )
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
                if existing_transaction.author == self.database_uuid {
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
                };
                transactions_to_insert.insert(change.transaction_id.clone(), transaction);
                changes_to_apply.push(change);
            }
        }

        // Insert new transactions
        for (_, transaction) in transactions_to_insert {
            log::debug!(
                "SQL EXECUTE: INSERT OR IGNORE INTO _transaction (id, timestamp, author) VALUES (?, ?, ?)"
            );
            let affected_rows = tx.execute(
                "INSERT OR IGNORE INTO _transaction (id, timestamp, author) VALUES (?, ?, ?)",
                rusqlite::params![transaction.id, transaction.timestamp, transaction.author],
            )?;
            log::debug!("SQL EXECUTE RESULT: {} rows affected", affected_rows);
        }

        // Insert change records
        for change in &changes_to_apply {
            log::debug!(
                "SQL EXECUTE: INSERT OR IGNORE INTO _change (transaction_id, entity_type, entity_key, attribute, old_value, new_value) VALUES (?, ?, ?, ?, ?, ?)"
            );
            let affected_rows = tx.execute(
                "INSERT OR IGNORE INTO _change (transaction_id, entity_type, entity_key, attribute, old_value, new_value) VALUES (?, ?, ?, ?, ?, ?)",
                rusqlite::params![
                    change.transaction_id,
                    change.entity_type,
                    change.entity_key,
                    change.attribute,
                    change.old_value,
                    change.new_value,
                ],
            )?;
            log::debug!("SQL EXECUTE RESULT: {} rows affected", affected_rows);
        }

        // Apply entity changes by reconstructing entities from attribute changes
        self.apply_attribute_changes(&tx, &changes_to_apply)?;

        tx.commit()?;
        Ok(())
    }

    fn get_transaction_by_id(
        &self,
        conn: &rusqlite::Connection,
        transaction_id: &str,
    ) -> anyhow::Result<Transaction> {
        let mut stmt =
            conn.prepare("SELECT id, timestamp, author FROM _transaction WHERE id = ?")?;
        let mut rows = stmt.query([transaction_id])?;

        if let Some(row) = rows.next()? {
            Ok(Transaction {
                id: row.get(0)?,
                timestamp: row.get(1)?,
                author: row.get(2)?,
            })
        } else {
            Err(anyhow::anyhow!("Transaction not found: {}", transaction_id))
        }
    }

    fn apply_attribute_changes(
        &self,
        tx: &rusqlite::Transaction,
        changes: &[&Change],
    ) -> anyhow::Result<()> {
        // Group changes by entity (type + key)
        let mut entities: std::collections::HashMap<(String, String), Vec<&Change>> = std::collections::HashMap::new();
        
        for change in changes {
            let entity_key = (change.entity_type.clone(), change.entity_key.clone());
            entities.entry(entity_key).or_default().push(change);
        }
        
        // Process each entity
        for ((entity_type, entity_key), entity_changes) in entities {
            // Get current entity state from database
            let current_entity = if self.record_exists(tx, &entity_type, &entity_key)? {
                Some(self.get_as_value(tx, &entity_type, &entity_key)?)
            } else {
                None
            };
            
            // Sort changes by transaction ID (UUIDv7 provides chronological ordering)
            let mut sorted_changes = entity_changes;
            sorted_changes.sort_by(|a, b| a.transaction_id.cmp(&b.transaction_id));
            
            // Apply changes to reconstruct entity
            let final_entity = self.reconstruct_entity_from_changes(current_entity, &sorted_changes)?;
            
            // Save the reconstructed entity
            match final_entity {
                Some(entity_json) => {
                    let table_columns = self.get_table_columns(tx, &entity_type)?;
                    let all_params = serde_rusqlite::to_params_named(&entity_json)?;
                    
                    if self.record_exists(tx, &entity_type, &entity_key)? {
                        self.update_record(tx, &entity_type, &entity_key, all_params, &table_columns)?;
                    } else {
                        self.insert_record(tx, &entity_type, all_params, &table_columns)?;
                    }
                }
                None => {
                    // Entity should be deleted
                    if self.record_exists(tx, &entity_type, &entity_key)? {
                        let sql = format!("DELETE FROM {} WHERE key = ?", entity_type);
                        tx.execute(&sql, [&entity_key])?;
                    }
                }
            }
        }
        
        Ok(())
    }
    
    fn reconstruct_entity_from_changes(
        &self,
        current_entity: Option<Value>,
        changes: &[&Change],
    ) -> anyhow::Result<Option<Value>> {
        // Start with current state or empty object
        let mut entity = current_entity.unwrap_or_else(|| serde_json::json!({}));
        
        for change in changes {
            if let Some(new_value) = &change.new_value {
                // Set attribute
                let parsed_value: Value = serde_json::from_str(new_value)?;
                entity[&change.attribute] = parsed_value;
            } else {
                // Remove attribute
                if let Some(obj) = entity.as_object_mut() {
                    obj.remove(&change.attribute);
                }
            }
        }
        
        // Return None if entity is empty (deleted)
        if entity.as_object().map_or(true, |obj| obj.is_empty()) {
            Ok(None)
        } else {
            Ok(Some(entity))
        }
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Clone, Default, Debug)]
    pub struct Artist {
        pub key: String,
        pub name: String,
        pub disambiguation: Option<String>,
    }

    #[derive(Serialize, Deserialize, Clone, Default, Debug)]
    pub struct ArtistWithExtra {
        pub key: String,
        pub name: String,
        pub disambiguation: Option<String>,
        pub extra_field: Option<String>, // This field doesn't exist in the Artist table
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

        // Save a new artist - should create attribute changes
        let artist = db.save(&Artist {
            name: "Metallica".to_string(),
            disambiguation: Some("American metal band".to_string()),
            ..Default::default()
        })?;

        // Query changes - should have one change per attribute
        let changes = db.get_changes_for_entity("Artist", &artist.key)?;
        assert_eq!(changes.len(), 3); // key, name, disambiguation

        // Verify all changes are for this entity and have no old values (insert)
        for change in &changes {
            assert_eq!(change.entity_type, "Artist");
            assert_eq!(change.entity_key, artist.key);
            assert!(change.old_value.is_none());
            assert!(change.new_value.is_some());
        }

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

        // Update only the disambiguation field
        let _artist2 = db.save(&Artist {
            key: artist1.key.clone(),
            name: "Iron Maiden".to_string(),
            disambiguation: Some("British heavy metal band".to_string()),
        })?;

        // Query changes for this entity
        let changes = db.get_changes_for_entity("Artist", &artist1.key)?;
        
        // Find the disambiguation changes
        let disambiguation_changes: Vec<_> = changes
            .iter()
            .filter(|c| c.attribute == "disambiguation")
            .collect();
        
        assert_eq!(disambiguation_changes.len(), 2); // Original + update
        
        // Verify the update has both old and new values
        let update_change = &disambiguation_changes[1];
        assert!(update_change.old_value.is_some());
        assert!(update_change.new_value.is_some());
        assert!(update_change.old_value.as_ref().unwrap().contains("British metal band"));
        assert!(update_change.new_value.as_ref().unwrap().contains("British heavy metal band"));

        Ok(())
    }

    #[test]
    fn test_save_with_change_tracking() -> anyhow::Result<()> {
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

        let iterations = 100;

        // Test our save() function with change tracking
        for i in 0..iterations {
            let test_artist = Artist {
                name: format!("Test Artist {}", i),
                disambiguation: Some("Test Band".to_string()),
                ..Default::default()
            };
            db.save(&test_artist)?;
        }

        // Verify we have the expected number of records
        let count: i64 = {
            let conn = db.conn.read().unwrap();
            conn.query_row("SELECT COUNT(*) FROM Artist", [], |row| row.get(0))?
        };
        assert_eq!(count, iterations as i64);

        // Verify change tracking is working
        let total_changes: i64 = {
            let conn = db.conn.read().unwrap();
            conn.query_row("SELECT COUNT(*) FROM _change", [], |row| row.get(0))?
        };
        // Each artist has 3 attributes (key, name, disambiguation), so 3 changes per artist
        assert_eq!(total_changes, (iterations * 3) as i64);

        Ok(())
    }
}