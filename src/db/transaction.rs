use anyhow::{anyhow, Result};
use rusqlite::{Params, ToSql, Transaction};
use serde_rusqlite::NamedParamSlice;
use uuid::Uuid;
use std::cell::RefCell;

use crate::db::{Db, DbEvent, Entity};

pub struct DbTransaction<'a> {
    db: &'a Db,
    txn: &'a Transaction<'a>,
    pending_events: RefCell<Vec<DbEvent>>,
}

pub type DbValue = NamedParamSlice;

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
    
    fn entity_to_value<E: Entity>(entity: &E, column_names: &[String]) -> Result<DbValue> {
        let column_name_refs: Vec<&str> = column_names.iter().map(String::as_str).collect();
        let params = serde_rusqlite::to_params_named_with_fields(entity, &column_name_refs)?;
        Ok(params)
    }

    fn save_internal<E: Entity>(&self, entity: &E, track_changes: bool) -> Result<E> {
        let table_name = self.db.table_name_for_type::<E>()?;
        let column_names = self.db.table_column_names(self.txn, &table_name)?;

        let mut new_value = Self::entity_to_value(entity, &column_names)?;
        let id = self.ensure_entity_id(&mut new_value)?;
        let old_value = self.get::<E>(&id)?
            .and_then(|e| Self::entity_to_value(&e, &column_names).ok());

        let exists = old_value.is_some();
        
        if exists {
            self.update_entity(&table_name, &column_names, &new_value)?;
        } else {
            self.insert_entity(&table_name, &column_names, &new_value)?;
        }
        
        // Track changes
        if track_changes {
            crate::changelog::track_changes(self, &table_name, &id, old_value.as_ref(), 
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

    fn ensure_entity_id(&self, entity_value: &mut DbValue) -> Result<String> {
        let id_param = entity_value.iter_mut()
            .find(|(name, _)| name == &":id")
            .ok_or_else(|| anyhow!("no id column on entity"))?;
        
        match Self::extract_id(&id_param.1).filter(|s| !s.is_empty()) {
            Some(id) => Ok(id),
            None => {
                let id = Uuid::now_v7().to_string();
                id_param.1 = Box::new(id.clone());
                Ok(id)
            }
        }
    }

    fn extract_id(val: &Box<dyn ToSql>) -> Option<String> {
        use rusqlite::types::{ToSqlOutput, Value, ValueRef};
        
        val.to_sql().ok().and_then(|output| match output {
            ToSqlOutput::Borrowed(ValueRef::Text(bytes)) => 
                String::from_utf8(bytes.to_vec()).ok(),
            ToSqlOutput::Owned(Value::Text(s)) => Some(s),
            _ => None,
        })
    }

    fn update_entity(&self, table_name: &str, column_names: &[String], entity_value: &DbValue) -> Result<()> {
        let set_clause = column_names
            .iter()
            .filter(|col| *col != "id")
            .map(|col| format!("{} = :{}", col, col))
            .collect::<Vec<_>>()
            .join(", ");
        
        let sql = format!("UPDATE {} SET {} WHERE id = :id", table_name, set_clause);

        self.execute_with_named_params(&sql, entity_value)
    }
    
    fn insert_entity(&self, table_name: &str, column_names: &[String], entity_value: &DbValue) -> Result<()> {
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
        self.execute_with_named_params(&sql, entity_value)
    }
    
    fn execute_with_named_params(&self, sql: &str, entity_value: &DbValue) -> Result<()> {
        let mut stmt = self.txn.prepare(sql)?;
        stmt.execute(entity_value.to_slice().as_slice())?;
        Ok(())
    }
    
    pub(crate) fn take_pending_events(&self) -> Vec<DbEvent> {
        std::mem::take(&mut *self.pending_events.borrow_mut())
    }
    
    pub(crate) fn add_pending_event(&self, event: DbEvent) {
        self.pending_events.borrow_mut().push(event);
    }
}
