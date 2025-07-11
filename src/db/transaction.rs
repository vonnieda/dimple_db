use anyhow::Result;
use rusqlite::{Params, Transaction};
use uuid::Uuid;

use crate::db::{Db, Entity, types::DbEvent};

pub struct DbTransaction<'a> {
    db: &'a Db,
    txn: &'a Transaction<'a>,
    id: String,
}

impl<'a> DbTransaction<'a> {
    pub(crate) fn new(db: &'a Db, txn: &'a Transaction<'a>) -> Self {
        Self {
            db,
            txn,
            id: Uuid::now_v7().to_string(),
        }
    }

    /// Saves the entity to the database. 
    /// 
    /// The entity's type name is used for the table name, and the table
    /// columns are mapped to the entity fields using serde_rusqlite. If an
    /// entity with the same id already exists it is updated, otherwise a new
    /// entity is inserted with a new uuidv7 for it's id. 
    /// 
    /// A diff between the old entity, if any, and the new is created and
    /// saved in the change tracking tables. Subscribers are then notified
    /// of the changes and the newly inserted or updated entity is returned.
    /// 
    /// Note that only fields present in both the table and entity are mapped.
    pub fn save<E: Entity>(&self, entity: &E) -> Result<E> {
        let table_name = self.db.table_name_for_type::<E>()?;
        let column_names = self.db.table_column_names(self.txn, &table_name)?;

        // Convert the entity to a JSON Value so we can manipulate it
        // generically without needing more than Serialize.
        let mut new_value = serde_json::to_value(entity)?;
        let id = self.ensure_entity_id(&mut new_value)?;        
        let old_value = self.get::<E>(&id)?
            .and_then(|e| serde_json::to_value(e).ok());

        let exists = old_value.is_some();
        
        if exists {
            self.update_entity(&table_name, &column_names, &new_value)?;
        } else {
            self.insert_entity(&table_name, &column_names, &new_value)?;
        }
        
        // Track changes
        self.track_changes(&table_name, &id, old_value.as_ref(), 
            &new_value, &column_names)?;
        
        // Notify subscribers
        let event = if exists {
            DbEvent::Update(table_name.clone(), id.clone())
        } else {
            DbEvent::Insert(table_name.clone(), id.clone())
        };
        self.db.notify_subscribers(event);
        
        self.get::<E>(&id)?
            .ok_or_else(|| anyhow::anyhow!("Failed to retrieve saved entity"))    
    }

    pub fn query<E: Entity, P: Params>(&self, sql: &str, params: P) -> Result<Vec<E>> {
        let mut stmt = self.txn.prepare(sql)?;
        let entities = serde_rusqlite::from_rows::<E>(stmt.query(params)?)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(entities)
    }

    pub fn get<E: Entity>(&self, id: &str) -> Result<Option<E>> {
        let table_name = self.db.table_name_for_type::<E>()?;
        let sql = format!("SELECT * FROM {} WHERE id = ? LIMIT 1", table_name);
        Ok(self.query::<E, _>(&sql, [id])?.into_iter().next())
    }

    fn ensure_entity_id(&self, entity_value: &mut serde_json::Value) -> Result<String> {
        match entity_value.get("id").and_then(|v| v.as_str()) {
            Some(id) if !id.is_empty() => Ok(id.to_string()),
            _ => {
                let new_id = Uuid::now_v7().to_string();
                entity_value["id"] = serde_json::Value::String(new_id.clone());
                Ok(new_id)
            }
        }
    }
    
    fn update_entity(&self, table_name: &str, column_names: &[String], entity_value: &serde_json::Value) -> Result<()> {
        let set_clause = column_names
            .iter()
            .filter(|col| *col != "id")
            .map(|col| format!("{} = :{}", col, col))
            .collect::<Vec<_>>()
            .join(", ");
        
        let sql = format!("UPDATE {} SET {} WHERE id = :id", table_name, set_clause);
        self.execute_with_named_params(&sql, entity_value, column_names)
    }
    
    fn insert_entity(&self, table_name: &str, column_names: &[String], entity_value: &serde_json::Value) -> Result<()> {
        let placeholders = column_names
            .iter()
            .map(|col| format!(":{}", col))
            .collect::<Vec<_>>()
            .join(", ");
        
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table_name,
            column_names.join(", "),
            placeholders
        );
        self.execute_with_named_params(&sql, entity_value, column_names)
    }
    
    fn execute_with_named_params(&self, sql: &str, entity_value: &serde_json::Value, column_names: &[String]) -> Result<()> {
        let mut stmt = self.txn.prepare(sql)?;
        let str_refs: Vec<&str> = column_names.iter().map(|s| s.as_str()).collect();
        let params = serde_rusqlite::to_params_named_with_fields(entity_value, &str_refs)?;
        stmt.execute(params.to_slice().as_slice())?;
        Ok(())
    }
    
    fn track_changes(&self, table_name: &str, entity_id: &str, 
            old_entity: Option<&serde_json::Value>, 
            new_entity: &serde_json::Value,
            column_names: &[String]) -> Result<()> {
        
    
        // First, ensure we have a transaction record
        // Get database_uuid to use as author
        let database_uuid: String = self.txn.query_row(
            "SELECT value FROM ZV_METADATA WHERE key = 'database_uuid'",
            [],
            |row| row.get(0)
        )?;
        
        self.txn.execute(
            "INSERT OR IGNORE INTO ZV_TRANSACTION (id, author) VALUES (?, ?)",
            [&self.id, &database_uuid]
        )?;
        
        // Track changes for each column (except id)
        for column_name in column_names {
            if column_name == "id" {
                continue;
            }
            
            // Convert JSON null to None, other values to their string representation
            let old_value = old_entity
                .and_then(|e| e.get(column_name))
                .and_then(|v| match v {
                    serde_json::Value::Null => None,
                    _ => Some(v.to_string())
                });
                
            let new_value = new_entity
                .get(column_name)
                .and_then(|v| match v {
                    serde_json::Value::Null => None,
                    _ => Some(v.to_string())
                });
            
            // Track all values on insert, only changes on update
            let is_insert = old_entity.is_none();
            let should_track = is_insert || (old_value != new_value);
            if should_track {
                let change_id = Uuid::now_v7().to_string();
                
                // Use rusqlite's support for Option to store NULL values properly
                self.txn.execute(
                    "INSERT INTO ZV_CHANGE (id, transaction_id, entity_type, entity_id, attribute, old_value, new_value) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    rusqlite::params![
                        &change_id,
                        &self.id,
                        table_name,
                        entity_id,
                        column_name,
                        old_value.as_deref(),
                        new_value.as_deref(),
                    ]
                )?;
            }
        }
        
        Ok(())
    }
}