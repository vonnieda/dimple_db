use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct Change {
    pub id: String,
    pub author_id: String,
    pub entity_type: String,
    pub entity_id: String,
    pub old_values: Option<String>,
    pub new_values: Option<String>,
    pub merged: bool,
}
