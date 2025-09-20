pub mod core;
pub mod query;
pub mod sql_parser;
pub mod transaction;

pub use core::*;
pub use query::*;
pub use rusqlite_migration::*;

use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// Trait for types that can be stored in the database
pub trait Entity: Serialize + DeserializeOwned {}

// Blanket implementation for any type that meets the requirements
impl<T> Entity for T where T: Serialize + DeserializeOwned {}

/// Sent to subscribers whenever the database is changed. Each variant includes
/// the entity_type and entity_id.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DbEvent {
    pub operation: DbEventOperation,
    pub entity_type: String,
    pub entity_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum DbEventOperation {
    Insert,
    Update,
}