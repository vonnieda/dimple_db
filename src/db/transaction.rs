use anyhow::Result;
use rusqlite::{Params, Transaction, params};
use uuid::Uuid;
use std::cell::RefCell;

use crate::db::{changelog, types::DbEvent, Db, Entity};

pub struct DbTransaction<'a> {
    db: &'a Db,
    txn: &'a Transaction<'a>,
    pending_events: RefCell<Vec<DbEvent>>,
}

impl<'a> DbTransaction<'a> {
    pub(crate) fn new(db: &'a Db, txn: &'a Transaction<'a>) -> Self {
        Self {
            db,
            txn,
            pending_events: RefCell::new(Vec::new()),
        }
    }

    pub fn db(&self) -> &Db {
        self.db
    }
    
    pub fn txn(&self) -> &rusqlite::Transaction {
        self.txn
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
        self.save_internal(entity, true)
    }

    pub fn save_untracked<E: Entity>(&self, entity: &E) -> Result<E> {
        self.save_internal(entity, false)
    }
    
    /// Save a dynamic entity when we know the table name but not the type.
    /// This is used by sync operations where we need to save entities without compile-time type information.
    pub fn save_dynamic(&self, table_name: &str, entity_data: &serde_json::Map<String, serde_json::Value>) -> Result<()> {
        let column_names = self.db.table_column_names(self.txn, table_name)?;
        
        // Convert map to Value
        let mut new_value = serde_json::Value::Object(entity_data.clone());
        let id = self.ensure_entity_id(&mut new_value)?;
        
        // Check if entity exists
        let check_sql = format!("SELECT COUNT(*) FROM {} WHERE id = ?", table_name);
        let exists: bool = self.txn.query_row(&check_sql, params![&id], |row| {
            row.get::<_, i64>(0).map(|count| count > 0)
        })?;
        
        if exists {
            self.update_entity(table_name, &column_names, &new_value)?;
        } else {
            self.insert_entity(table_name, &column_names, &new_value)?;
        }
        
        // Queue event for notification after commit
        let event = if exists {
            DbEvent::Update(table_name.to_string(), id.clone())
        } else {
            DbEvent::Insert(table_name.to_string(), id.clone())
        };
        self.pending_events.borrow_mut().push(event);
        
        Ok(())
    }

    fn save_internal<E: Entity>(&self, entity: &E, track_changes: bool) -> Result<E> {
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
        if track_changes {
            changelog::track_changes(self, &table_name, &id, old_value.as_ref(), 
                &new_value, &column_names)?;
        }
        
        // Queue event for notification after commit
        let event = if exists {
            DbEvent::Update(table_name.clone(), id.clone())
        } else {
            DbEvent::Insert(table_name.clone(), id.clone())
        };
        self.pending_events.borrow_mut().push(event);
        
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
    
    pub(crate) fn take_pending_events(&self) -> Vec<DbEvent> {
        std::mem::take(&mut *self.pending_events.borrow_mut())
    }
}