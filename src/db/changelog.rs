use anyhow::Result;
use rusqlite::Connection;
use uuid::Uuid;

use crate::db::transaction::DbTransaction;

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