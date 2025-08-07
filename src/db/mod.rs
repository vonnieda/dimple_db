pub mod core;
pub mod query;
pub mod transaction;

pub use core::*;
pub use query::*;
pub use rusqlite_migration::*;

use serde::{Serialize, de::DeserializeOwned};

/// Trait for types that can be stored in the database
pub trait Entity: Serialize + DeserializeOwned {}

// Blanket implementation for any type that meets the requirements
impl<T> Entity for T where T: Serialize + DeserializeOwned {}

/// Sent to subscribers whenever the database is changed. Each variant includes
/// the entity_type and entity_id.
#[derive(Clone, Debug)]
pub enum DbEvent {
    Insert(String, String),
    Update(String, String),
}