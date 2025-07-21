use anyhow::Result;
use rusqlite::Connection;
use uuid::Uuid;

use crate::db::transaction::DbTransaction;

/// ZV is used as a prefix for the internal tables. Z puts them
/// at the end of alphabetical lists and V differentiates them from
/// Core Data tables.
pub (crate) fn init_change_tracking_tables(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS ZV_METADATA (
            key TEXT NOT NULL PRIMARY KEY,
            value TEXT NOT NULL
        );

        INSERT OR IGNORE INTO ZV_METADATA (key, value) 
            VALUES ('database_uuid', uuid7());

        CREATE TABLE IF NOT EXISTS ZV_CHANGE (
            id TEXT NOT NULL PRIMARY KEY,
            author_id TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            columns_json TEXT,
            merged BOOL NOT NULL DEFAULT FALSE
        );
    ")?;
    Ok(())
}

pub (crate) fn track_changes(txn: &DbTransaction, table_name: &str, entity_id: &str, 
        old_entity: Option<&serde_json::Value>, 
        new_entity: &serde_json::Value,
        column_names: &[String]) -> Result<()> {
    
    let author_id = txn.db().get_database_uuid()?;
    
    // Compute the diff between old and new entities
    let columns_json_map = compute_entity_changes(old_entity, new_entity, column_names);
    
    // Only create a change record if there are actual changes
    if !columns_json_map.is_empty() {
        let change_id = Uuid::now_v7().to_string();
        
        // Convert map to JSON string
        let columns_json_str = Some(serde_json::to_string(&columns_json_map)?);
        
        txn.txn().execute(
            "INSERT INTO ZV_CHANGE (id, author_id, entity_type, entity_id, columns_json) VALUES (?, ?, ?, ?, ?)",
            rusqlite::params![
                &change_id,
                &author_id,
                table_name,
                entity_id,
                columns_json_str,
            ]
        )?;
    }
    
    Ok(())
}

/// Compute the changes to track, returning only changed/new fields
fn compute_entity_changes(old_entity: Option<&serde_json::Value>, 
                          new_entity: &serde_json::Value,
                          column_names: &[String]) -> serde_json::Map<String, serde_json::Value> {
    let mut columns_json = serde_json::Map::new();
    
    for column_name in column_names {
        if column_name == "id" {
            continue;
        }
        
        let old_value = old_entity.and_then(|e| e.get(column_name));
        let new_value = new_entity.get(column_name);
        
        // Track all values on insert, only changes on update
        let is_insert = old_entity.is_none();
        let values_differ = old_value != new_value;
        
        if is_insert || values_differ {
            if let Some(new_val) = new_value {
                columns_json.insert(column_name.clone(), new_val.clone());
            } else {
                // Handle null values
                columns_json.insert(column_name.clone(), serde_json::Value::Null);
            }
        }
    }
    
    columns_json
}


#[cfg(test)]
mod tests {
    use anyhow::Result;
    use rusqlite_migration::{Migrations, M};
    use serde::{Deserialize, Serialize};
    
    use crate::{Db, db::ChangeRecord};

    #[derive(Serialize, Deserialize, Clone, Debug, Default)]
    struct Artist {
        pub id: String,
        pub name: String,
        pub summary: Option<String>,
    }

    fn setup_db() -> Result<Db> {
        let db = Db::open_memory()?;
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (name TEXT NOT NULL, summary TEXT, id TEXT NOT NULL PRIMARY KEY);"),
        ]);
        db.migrate(&migrations)?;
        Ok(db)
    }

    fn get_changes(db: &Db, entity_id: &str) -> Result<Vec<ChangeRecord>> {
        db.query(
            "SELECT id, author_id, entity_type, entity_id, columns_json, merged 
             FROM ZV_CHANGE WHERE entity_id = ? ORDER BY id",
            [entity_id]
        )
    }

    #[test]
    fn insert_creates_change_records() -> Result<()> {
        let db = setup_db()?;
        let artist = db.save(&Artist { name: "Radiohead".to_string(), ..Default::default() })?;
        
        let changes = get_changes(&db, &artist.id)?;
        assert_eq!(changes.len(), 1); // One change record for the entity
        assert!(changes[0].columns_json.is_some());
        
        // Check that columns_json contains the name
        let columns: serde_json::Value = serde_json::from_str(&changes[0].columns_json.as_ref().unwrap())?;
        assert_eq!(columns["name"], "Radiohead");
        Ok(())
    }

    #[test]
    fn update_only_tracks_modified_fields() -> Result<()> {
        let db = setup_db()?;
        let artist = db.save(&Artist { name: "Radiohead".to_string(), ..Default::default() })?;
        
        db.save(&Artist {
            id: artist.id.clone(),
            name: "Radiohead".to_string(), // unchanged
            summary: Some("Rock band".to_string()), // changed
        })?;
        
        let changes = get_changes(&db, &artist.id)?;
        assert_eq!(changes.len(), 2); // insert + update
        
        // Check the update change only contains the modified field
        let update_columns: serde_json::Value = serde_json::from_str(&changes[1].columns_json.as_ref().unwrap())?;
        assert_eq!(update_columns["summary"], "Rock band");
        assert!(update_columns.get("name").is_none()); // name wasn't changed
        Ok(())
    }

    #[test]
    fn change_structs_work_with_query() -> Result<()> {
        let db = setup_db()?;
        let artist = db.save(&Artist { name: "Pink Floyd".to_string(), ..Default::default() })?;
        
        let changes = get_changes(&db, &artist.id)?;
        
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].entity_type, "Artist");
        assert_eq!(changes[0].author_id, db.get_database_uuid()?);
        Ok(())
    }

    #[test]
    fn null_values_included_in_changes() -> Result<()> {
        let db = setup_db()?;
        
        // Save an entity with an explicit null value
        let artist = db.save(&Artist { 
            name: "Test Artist".to_string(), 
            summary: None, // This should be included as null in changes
            ..Default::default() 
        })?;
        
        let changes = get_changes(&db, &artist.id)?;
        assert_eq!(changes.len(), 1);
        assert!(changes[0].columns_json.is_some());
        
        // Parse the columns_json to check it includes the null field
        let columns: serde_json::Value = serde_json::from_str(&changes[0].columns_json.as_ref().unwrap())?;
        assert_eq!(columns["name"], "Test Artist");
        // The key point: summary should be present as null, not missing
        assert!(columns.get("summary").is_some(), "summary field should be present");
        assert!(columns["summary"].is_null(), "summary field should be null");
        
        Ok(())
    }

    #[test]
    fn null_to_value_change_tracking() -> Result<()> {
        let db = setup_db()?;
        
        // Start with an entity that has a null summary
        let artist = db.save(&Artist { 
            name: "Test Artist".to_string(), 
            summary: None,
            ..Default::default() 
        })?;
        
        // Update it to have a non-null summary
        let updated_artist = db.save(&Artist {
            id: artist.id.clone(),
            name: "Test Artist".to_string(),
            summary: Some("Now has a summary".to_string()),
        })?;
        
        let changes = get_changes(&db, &updated_artist.id)?;
        assert_eq!(changes.len(), 2); // insert + update
        
        // Check the update change
        let update_change = &changes[1];
        let columns: serde_json::Value = serde_json::from_str(&update_change.columns_json.as_ref().unwrap())?;
        
        // Should track the change to "Now has a summary"
        assert_eq!(columns["summary"], "Now has a summary");
        
        Ok(())
    }
}