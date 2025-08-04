use anyhow::Result;

use crate::{changelog::Changelog, db::ChangelogChangeWithFields, Db};

/// TODO: bring the db changelog code over to this, such as the table init and
/// track_changes function. Then in Db we're just creating a DbChangelog and
/// using it. So clean.

pub struct DbChangelog {
    db: Db,
}

impl DbChangelog {
    pub fn new(db: Db) -> Self {
        Self { db }
    }
}

impl Changelog for DbChangelog {
    fn get_all_change_ids(&self) -> Result<Vec<String>> {
        let changes = self.db.query::<crate::db::ChangelogChange, _>(
            "SELECT id, author_id, entity_type, entity_id, merged FROM ZV_CHANGE ORDER BY id ASC", 
            ()
        )?;
        Ok(changes.into_iter().map(|c| c.id).collect())
    }
    
    fn get_changes_after(&self, after_id: Option<&str>) -> Result<Vec<ChangelogChangeWithFields>> {
        let changes = if let Some(after) = after_id {
            self.db.query::<crate::db::ChangelogChange, _>(
                "SELECT id, author_id, entity_type, entity_id, merged FROM ZV_CHANGE WHERE id > ? ORDER BY id ASC",
                (after,)
            )?
        } else {
            self.db.query::<crate::db::ChangelogChange, _>(
                "SELECT id, author_id, entity_type, entity_id, merged FROM ZV_CHANGE ORDER BY id ASC",
                ()
            )?
        };
        
        let mut remote_changes = Vec::new();
        for change in changes {
            let fields = self.db.transaction(|txn| {
                let mut stmt = txn.txn().prepare(
                    "SELECT field_name, field_value FROM ZV_CHANGE_FIELD WHERE change_id = ?"
                )?;
                let mut rows = stmt.query([&change.id])?;
                
                let mut fields = Vec::new();
                while let Some(row) = rows.next()? {
                    let field_name: String = row.get(0)?;
                    let sql_value: rusqlite::types::Value = row.get_ref(1)?.into();
                    
                    fields.push(crate::db::RemoteFieldRecord {
                        field_name,
                        field_value: crate::sync::sync_engine::sql_value_to_msgpack(&sql_value),
                    });
                }
                Ok(fields)
            })?;
            
            remote_changes.push(ChangelogChangeWithFields { change, fields });
        }
        
        Ok(remote_changes)
    }
    
    fn append_changes(&self, changes: Vec<ChangelogChangeWithFields>) -> Result<()> {
        self.db.transaction(|txn| {
            for remote_change in changes {
                let change = &remote_change.change;
                
                // Insert the change record
                txn.txn().execute(
                    "INSERT OR IGNORE INTO ZV_CHANGE (id, author_id, entity_type, entity_id, merged) 
                     VALUES (?, ?, ?, ?, false)",
                    rusqlite::params![
                        &change.id,
                        &change.author_id,
                        &change.entity_type,
                        &change.entity_id,
                    ]
                )?;
                
                // Insert the field records
                for field in &remote_change.fields {
                    let sql_value = crate::sync::sync_engine::msgpack_to_sql_value(&field.field_value);
                    txn.txn().execute(
                        "INSERT OR IGNORE INTO ZV_CHANGE_FIELD (change_id, field_name, field_value) VALUES (?, ?, ?)",
                        rusqlite::params![
                            &change.id,
                            &field.field_name,
                            &sql_value,
                        ]
                    )?;
                }
            }
            Ok(())
        })
    }
    
    fn has_change(&self, change_id: &str) -> Result<bool> {
        let results = self.db.query::<crate::db::ChangelogChange, _>(
            "SELECT id, author_id, entity_type, entity_id, merged FROM ZV_CHANGE WHERE id = ?",
            (change_id,)
        )?;
        Ok(!results.is_empty())
    }
}