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
    pub merged: bool,
}

/// Represents a field change record in the ZV_CHANGE_FIELD table
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChangeFieldRecord {
    pub change_id: String,
    pub field_name: String,
    pub field_value: Vec<u8>,
}

/// Combined record for remote storage containing change + field records
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RemoteChangeRecord {
    pub change: ChangeRecord,
    pub fields: Vec<RemoteFieldRecord>,
}

/// Simplified field record for remote storage (no change_id since it's in the parent)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RemoteFieldRecord {
    pub field_name: String,
    pub field_value: rmpv::Value,
}

/// Sent to subscribers whenever the database is changed. Each variant includes
/// the entity_type and entity_id.
#[derive(Clone, Debug)]
pub enum DbEvent {
    Insert(String, String),
    Update(String, String),
    Delete(String, String), // Currently unimplemented, may come with tombstones.
}