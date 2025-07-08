use anyhow::Result;
use serde::{Serialize, de::DeserializeOwned};

pub trait Entity: Serialize + DeserializeOwned {}

// Blanket implementation for any type that meets the requirements
impl<T> Entity for T where T: Serialize + DeserializeOwned {}

pub struct DbTransaction {

}

impl DbTransaction {
    pub fn save<T: Entity>(&self, entity: &T) -> Result<T> {
        // let table_name = self.type_name::<T>();
        // create a transaction
        // get the table's column names
        // map the entity's fields to columns
        // insert or update the value, creating a uuidv7 id if needed
        // record the change in history tables
        // notify listeners
        todo!()
    }

    // pub fn query<T: Entity, P: Params>(&self, sql: &str, params: P) -> anyhow::Result<Vec<T>> {
    //     // run the query, use serde_rusqlite to convert back to entities
    //     todo!()
    // }

    // fn query_one<T: Entity, P: Params>(&self, sql: &str, params: P) -> anyhow::Result<Option<T>> {
    //     // shortcut for query().first()
    //     todo!()
    // }

    // fn query_subscribe<T: Entity, P: Params, F: FnMut(Vec<T>) -> ()>(&self, sql: &str, params: P, cb: F) -> anyhow::Result<()> {
    //     // run an explain query plan and extract names of tables that the query depends on
    //     // and then when there are changes in any of those tables, re-run the query and
    //     // call the callback with the results
    //     todo!()
    // }

    fn delete<T: Entity>(&self, entity: &T) -> anyhow::Result<()> {
        todo!()
    }
}
