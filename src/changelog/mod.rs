pub mod changelog;
pub mod basic_storage_changelog;
pub mod batching_storage_changelog;
pub mod db_changelog;

pub use changelog::*;
use serde::{Deserialize, Serialize};
pub use basic_storage_changelog::BasicStorageChangelog;
pub use batching_storage_changelog::BatchingStorageChangelog;
pub use db_changelog::*;

/// Represents a change record in the ZV_CHANGE table
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChangelogChange {
    pub id: String,
    pub author_id: String,
    pub entity_type: String,
    pub entity_id: String,
    pub merged: bool,
}

/// Represents a field change record in the ZV_CHANGE_FIELD table
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChangelogField {
    pub change_id: String,
    pub field_name: String,
    pub field_value: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChangelogChangeWithFields {
    pub change: ChangelogChange,
    pub fields: Vec<RemoteFieldRecord>,
}

/// Simplified field record for remote storage (no change_id since it's in the parent)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RemoteFieldRecord {
    pub field_name: String,
    pub field_value: rmpv::Value,
}
