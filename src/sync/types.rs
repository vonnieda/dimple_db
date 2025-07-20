use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct ReplicaMetadata {
    pub replica_id: String,
    pub latest_change_id: String,
    pub latest_change_file: String,
}

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct Changes {
    pub replica_id: String,
    pub changes: Vec<Change>,
}

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct Change {
    pub id: String,
    pub author_id: String,
    pub entity_type: String,
    pub entity_id: String,
    pub old_values: Option<String>,
    pub new_values: Option<String>,
}
