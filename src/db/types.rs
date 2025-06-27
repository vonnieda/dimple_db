use std::collections::{HashMap, HashSet};
use std::sync::{RwLock, Weak};

use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub trait Entity: Serialize + DeserializeOwned {}

// Blanket implementation for any type that meets the requirements
impl<T> Entity for T where T: Serialize + DeserializeOwned {}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub id: String,
    pub timestamp: i64,
    pub author: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Change {
    pub transaction_id: String,
    pub entity_type: String,
    pub entity_key: String,
    pub attribute: String,
    pub old_value: Option<String>,
    pub new_value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult<T> {
    pub data: Vec<T>,
    pub keys: HashSet<String>,
    pub hash: u64,
}

#[derive(Debug, Clone)]
pub struct QuerySubscription {
    pub id: String,
    pub sql: String,
    pub params: Vec<String>, // Serialized parameters
    pub dependent_tables: HashSet<String>,
    pub last_result_keys: HashSet<String>,
    pub last_result_hash: u64,
}


pub struct QueryObserver {
    pub(crate) subscription_id: String,
    pub(crate) db: Weak<RwLock<HashMap<String, QuerySubscription>>>,
}

impl Drop for QueryObserver {
    fn drop(&mut self) {
        if let Some(subscriptions) = self.db.upgrade() {
            if let Ok(mut subs) = subscriptions.write() {
                subs.remove(&self.subscription_id);
            }
        }
    }
}

