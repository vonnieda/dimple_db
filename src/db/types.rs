use serde::{Deserialize, Serialize, de::DeserializeOwned};

/// Trait for types that can be stored in the database
pub trait Entity: Serialize + DeserializeOwned {}

// Blanket implementation for any type that meets the requirements
impl<T> Entity for T where T: Serialize + DeserializeOwned {}

/// Represents a change record in the ZV_CHANGE table
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChangeRecord {
    pub id: String,
    pub author_id: String,
    pub entity_type: String,
    pub entity_id: String,
    pub old_values: Option<String>,
    pub new_values: Option<String>,
}

/// Sent to subscribers whenever the database is changed. Each variant includes
/// the entity_name and entity_id.
#[derive(Clone, Debug)]
pub enum DbEvent {
    Insert(String, String),
    Update(String, String),
    Delete(String, String), // Currently unimplemented, may come with tombstones.
}