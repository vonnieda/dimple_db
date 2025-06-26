use uuid::Uuid;

use super::core::Db;
use super::types::{Entity, ChangeType, Change, Transaction};

impl Db {
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
}