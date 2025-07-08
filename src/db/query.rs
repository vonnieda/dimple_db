use anyhow::Result;
use rusqlite::Params;

use crate::Entity;


pub struct DbQuery {
}

impl DbQuery {
    pub fn query<T: Entity, P: Params>(&self, sql: &str, params: P) -> Result<Vec<T>> {
        // run the query, use serde_rusqlite to convert back to entities
        todo!()
    }

    pub fn one<T: Entity, P: Params>(&self, sql: &str, params: P) -> Result<Option<T>> {
        // shortcut for query().first()
        todo!()
    }

    pub fn subscribe<T: Entity, F: FnMut(Vec<T>) -> ()>(&self, cb: F) -> Result<()> {
        // run an explain query plan and extract names of tables that the query depends on
        // and then when there are changes in any of those tables, re-run the query and
        // call the callback with the results
        todo!()
    }
}

