use std::collections::HashSet;

use anyhow::Result;
use rusqlite::{Connection, OptionalExtension};
use uuid::Uuid;

use crate::{db::transaction::DbTransaction, Db};

/// ZV is used as a prefix for the internal tables. Z puts them
/// at the end of alphabetical lists and V differentiates them from
/// Core Data tables.
pub (crate) fn init_change_tracking_tables(conn: &Connection) -> Result<()> {
    // TODO I don't think we need old_values, so drop it and change new_values
    // to columns_json.
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
            old_values TEXT,
            new_values TEXT,
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
    let (old_values, new_values) = compute_entity_diff(old_entity, new_entity, column_names);
    
    // Only create a change record if there are actual changes
    if !old_values.is_empty() || !new_values.is_empty() {
        let change_id = Uuid::now_v7().to_string();
        
        // Convert maps to JSON strings
        let old_values_str = if old_values.is_empty() { 
            None 
        } else { 
            Some(serde_json::to_string(&old_values)?) 
        };
        
        let new_values_str = if new_values.is_empty() { 
            None 
        } else { 
            Some(serde_json::to_string(&new_values)?) 
        };
        
        txn.txn().execute(
            "INSERT INTO ZV_CHANGE (id, author_id, entity_type, entity_id, old_values, new_values) VALUES (?, ?, ?, ?, ?, ?)",
            rusqlite::params![
                &change_id,
                &author_id,
                table_name,
                entity_id,
                old_values_str,
                new_values_str,
            ]
        )?;
    }
    
    Ok(())
}

/// Compute the diff between old and new entities, returning only changed fields
fn compute_entity_diff(old_entity: Option<&serde_json::Value>, 
                        new_entity: &serde_json::Value,
                        column_names: &[String]) -> (serde_json::Map<String, serde_json::Value>, 
                                                    serde_json::Map<String, serde_json::Value>) {
    let mut old_values = serde_json::Map::new();
    let mut new_values = serde_json::Map::new();
    
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
            if let Some(old_val) = old_value {
                old_values.insert(column_name.clone(), old_val.clone());
            }
            if let Some(new_val) = new_value {
                new_values.insert(column_name.clone(), new_val.clone());
            }
        }
    }
    
    (old_values, new_values)
}

/// Reads the current state of the entity from the accumulated changelog.
/// TODO not finished/tested yet
pub (crate) fn get_entity_state(db: &Db, entity_name: &str, entity_id: &str) -> Result<serde_json::Value> {
    db.transaction(|txn| {
        // Get table column info from database
        let mut stmt = txn.txn().prepare(&format!(
            "SELECT name FROM pragma_table_info('{}') WHERE name != 'id'",
            entity_name
        ))?;
        
        let columns: Vec<String> = stmt.query_map([], |row| {
            row.get::<_, String>(0)
        })?.collect::<Result<Vec<_>, _>>()?;
        
        // Start with the id field
        let mut entity_state = serde_json::Map::new();
        entity_state.insert("id".to_string(), serde_json::Value::String(entity_id.to_string()));
        
        // Track which attributes have been set
        let mut attributes_needed: HashSet<String> = columns.into_iter().collect();
        
        // Read all changes for this entity, ordered newest to oldest
        let mut stmt = txn.txn().prepare(
            "SELECT new_values, old_values
                FROM ZV_CHANGE 
                WHERE entity_type = ? AND entity_id = ?
                ORDER BY id DESC"
        )?;
        
        let changes = stmt.query_map(
            rusqlite::params![entity_name, entity_id],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?
                ))
            }
        )?;
        
        // Process changes newest to oldest
        for change_result in changes {
            let (new_values_json, old_values_json) = change_result?;
            
            // Process new_values first (they take precedence)
            if let Some(new_json) = new_values_json {
                if let Ok(new_values) = serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&new_json) {
                    for (key, value) in new_values {
                        // Only set if we haven't already set this attribute
                        if attributes_needed.remove(&key) {
                            entity_state.insert(key, value);
                        }
                    }
                }
            }
            
            // Check if all attributes have been found
            if attributes_needed.is_empty() {
                break;
            }
        }
        
        // For any remaining attributes not found in changes, check old_values from the oldest change
        // This handles the initial insert case
        if !attributes_needed.is_empty() {
            let mut stmt = txn.txn().prepare(
                "SELECT old_values
                    FROM ZV_CHANGE 
                    WHERE entity_type = ? AND entity_id = ?
                    ORDER BY id ASC
                    LIMIT 1"
            )?;
            
            if let Ok(Some(old_values_json)) = stmt.query_row(
                rusqlite::params![entity_name, entity_id],
                |row| row.get::<_, Option<String>>(0)
            ).optional() {
                if let Some(old_json) = old_values_json {
                    if let Ok(old_values) = serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&old_json) {
                        for (key, value) in old_values {
                            if attributes_needed.contains(&key) && !entity_state.contains_key(&key) {
                                entity_state.insert(key, value);
                            }
                        }
                    }
                }
            }
        }
        
        Ok(serde_json::Value::Object(entity_state))
    })
}

#[cfg(test)]
mod tests {
    use rusqlite_migration::{Migrations, M};

    use crate::Db;

    // #[test]
    // fn get_entity_state() -> anyhow::Result<()> {
    //     let migrations = Migrations::new(vec![
    //         M::up("CREATE TABLE Artist (name TEXT NOT NULL, country TEXT, id TEXT NOT NULL PRIMARY KEY);"),
    //     ]);
    //     let db1 = Db::open_memory()?;
    //     let db2 = Db::open_memory()?;
    //     db1.migrate(&migrations)?;
    //     db2.migrate(&migrations)?;
        
    //     let artist = db1.save(&Artist {
    //         name: "Metallica".to_string(),
    //         ..Default::default()
    //     })?;

    //     // let artist_json = 
    //     todo!()
    // }
}