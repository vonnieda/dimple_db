use anyhow::Result;
use rusqlite::Connection;
use uuid::Uuid;
use std::collections::BTreeMap;
use rmpv::Value as MsgPackValue;

use crate::db::transaction::{DbTransaction, DbValue};

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
            merged BOOL NOT NULL DEFAULT FALSE
        );

        CREATE TABLE IF NOT EXISTS ZV_CHANGE_FIELD (
            change_id TEXT NOT NULL,
            field_name TEXT NOT NULL,
            field_value ANY,
            PRIMARY KEY (change_id, field_name),
            FOREIGN KEY (change_id) REFERENCES ZV_CHANGE(id)
        );
    ")?;
    Ok(())
}

pub (crate) fn track_changes(txn: &DbTransaction, table_name: &str, entity_id: &str, 
        old_entity: Option<&DbValue>, 
        new_entity: &DbValue,
        column_names: &[String]) -> Result<()> {
    
    let author_id = txn.db().get_database_uuid()?;
    
    // Compute the diff between old and new entities
    let field_changes = compute_entity_changes(old_entity, new_entity, column_names);
    
    // Only create a change record if there are actual changes
    if !field_changes.is_empty() {
        let change_id = Uuid::now_v7().to_string();
        
        // Insert the change record
        txn.txn().execute(
            "INSERT INTO ZV_CHANGE (id, author_id, entity_type, entity_id, merged) VALUES (?, ?, ?, ?, true)",
            rusqlite::params![
                &change_id,
                &author_id,
                table_name,
                entity_id,
            ]
        )?;
        
        // Insert individual field changes
        for (field_name, sql_value) in field_changes {
            txn.txn().execute(
                "INSERT INTO ZV_CHANGE_FIELD (change_id, field_name, field_value) VALUES (?, ?, ?)",
                rusqlite::params![
                    &change_id,
                    &field_name,
                    &sql_value,
                ]
            )?;
        }
    }
    
    Ok(())
}

/// Convert DbValue to a map for easier access
fn dbvalue_to_map(db_value: &DbValue) -> BTreeMap<String, rusqlite::types::Value> {
    let mut map = BTreeMap::new();
    for (name, value) in db_value.iter() {
        // Remove the : prefix from parameter names
        let clean_name = name.strip_prefix(':').unwrap_or(name);
        if let Ok(sql_value) = value.to_sql() {
            let val = match sql_value {
                rusqlite::types::ToSqlOutput::Borrowed(val) => val.into(),
                rusqlite::types::ToSqlOutput::Owned(val) => val,
                _ => continue,
            };
            map.insert(clean_name.to_string(), val);
        }
    }
    map
}

/// Convert a rusqlite::Value to a MessagePack Value
pub fn sql_value_to_msgpack(value: &rusqlite::types::Value) -> MsgPackValue {
    match value {
        rusqlite::types::Value::Null => MsgPackValue::Nil,
        rusqlite::types::Value::Integer(i) => MsgPackValue::Integer((*i).into()),
        rusqlite::types::Value::Real(f) => MsgPackValue::F64(*f),
        rusqlite::types::Value::Text(s) => MsgPackValue::String(s.clone().into()),
        rusqlite::types::Value::Blob(b) => MsgPackValue::Binary(b.clone()),
    }
}

/// Convert a MessagePack Value back to a rusqlite::Value
pub fn msgpack_to_sql_value(value: &MsgPackValue) -> rusqlite::types::Value {
    match value {
        MsgPackValue::Nil => rusqlite::types::Value::Null,
        MsgPackValue::Boolean(b) => rusqlite::types::Value::Integer(*b as i64),
        MsgPackValue::Integer(i) => {
            if let Some(i64_val) = i.as_i64() {
                rusqlite::types::Value::Integer(i64_val)
            } else if let Some(u64_val) = i.as_u64() {
                rusqlite::types::Value::Integer(u64_val as i64)
            } else {
                rusqlite::types::Value::Null
            }
        },
        MsgPackValue::F32(f) => rusqlite::types::Value::Real(*f as f64),
        MsgPackValue::F64(f) => rusqlite::types::Value::Real(*f),
        MsgPackValue::String(s) => {
            if let Some(string) = s.as_str() {
                rusqlite::types::Value::Text(string.to_string())
            } else {
                rusqlite::types::Value::Null
            }
        },
        MsgPackValue::Binary(b) => rusqlite::types::Value::Blob(b.clone()),
        _ => rusqlite::types::Value::Null,
    }
}

/// Compute the changes to track, returning only changed/new fields
fn compute_entity_changes(old_entity: Option<&DbValue>, 
                          new_entity: &DbValue,
                          column_names: &[String]) -> BTreeMap<String, rusqlite::types::Value> {
    let mut field_changes = BTreeMap::new();
    
    let old_map = old_entity.map(dbvalue_to_map);
    let new_map = dbvalue_to_map(new_entity);
    
    for column_name in column_names {
        if column_name == "id" {
            continue;
        }
        
        let old_value = old_map.as_ref().and_then(|m| m.get(column_name));
        let new_value = new_map.get(column_name);
        
        // Track all values on insert, only changes on update
        let is_insert = old_entity.is_none();
        let values_differ = old_value != new_value;
        
        if is_insert || values_differ {
            if let Some(new_val) = new_value {
                field_changes.insert(column_name.clone(), new_val.clone());
            } else {
                // Handle null values
                field_changes.insert(column_name.clone(), rusqlite::types::Value::Null);
            }
        }
    }
    
    field_changes
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
            "SELECT id, author_id, entity_type, entity_id, merged 
             FROM ZV_CHANGE WHERE entity_id = ? ORDER BY id",
            [entity_id]
        )
    }
    
    struct TestFieldRecord {
        field_name: String,
        field_value: rusqlite::types::Value,
    }
    
    fn get_change_fields(db: &Db, change_id: &str) -> Result<Vec<TestFieldRecord>> {
        db.transaction(|txn| {
            let mut stmt = txn.txn().prepare(
                "SELECT field_name, field_value FROM ZV_CHANGE_FIELD WHERE change_id = ? ORDER BY field_name"
            )?;
            let mut rows = stmt.query([change_id])?;
            
            let mut fields = Vec::new();
            while let Some(row) = rows.next()? {
                fields.push(TestFieldRecord {
                    field_name: row.get(0)?,
                    field_value: row.get(1)?,
                });
            }
            Ok(fields)
        })
    }
    
    fn get_field_value_as_string(field_record: &TestFieldRecord) -> String {
        match &field_record.field_value {
            rusqlite::types::Value::Text(s) => s.clone(),
            rusqlite::types::Value::Integer(i) => i.to_string(),
            rusqlite::types::Value::Real(f) => f.to_string(),
            rusqlite::types::Value::Null => "null".to_string(),
            rusqlite::types::Value::Blob(_) => "<blob>".to_string(),
        }
    }

    #[test]
    fn insert_creates_change_records() -> Result<()> {
        let db = setup_db()?;
        let artist = db.save(&Artist { name: "Radiohead".to_string(), ..Default::default() })?;
        
        let changes = get_changes(&db, &artist.id)?;
        assert_eq!(changes.len(), 1); // One change record for the entity
        
        // Check that change fields contain the name
        let fields = get_change_fields(&db, &changes[0].id)?;
        assert!(!fields.is_empty());
        
        let name_field = fields.iter().find(|f| f.field_name == "name").unwrap();
        assert_eq!(get_field_value_as_string(name_field), "Radiohead");
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
        let update_fields = get_change_fields(&db, &changes[1].id)?;
        assert_eq!(update_fields.len(), 1); // Only summary should have changed
        
        let summary_field = &update_fields[0];
        assert_eq!(summary_field.field_name, "summary");
        assert_eq!(get_field_value_as_string(summary_field), "Rock band");
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
        
        // Parse the change fields to check it includes the null field
        let fields = get_change_fields(&db, &changes[0].id)?;
        
        let name_field = fields.iter().find(|f| f.field_name == "name").unwrap();
        assert_eq!(get_field_value_as_string(name_field), "Test Artist");
        
        // The key point: summary should be present as null, not missing
        let summary_field = fields.iter().find(|f| f.field_name == "summary");
        assert!(summary_field.is_some(), "summary field should be present");
        assert_eq!(get_field_value_as_string(summary_field.unwrap()), "null", "summary field should be null");
        
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
        let update_fields = get_change_fields(&db, &update_change.id)?;
        
        // Should track the change to "Now has a summary"
        let summary_field = update_fields.iter().find(|f| f.field_name == "summary").unwrap();
        assert_eq!(get_field_value_as_string(summary_field), "Now has a summary");
        
        Ok(())
    }
}