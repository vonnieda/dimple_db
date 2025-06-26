use std::collections::{HashMap, HashSet};
use std::sync::mpsc::Receiver;
use std::sync::{RwLock, Weak};

use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub trait Entity: Serialize + DeserializeOwned {}

// Blanket implementation for any type that meets the requirements
impl<T> Entity for T where T: Serialize + DeserializeOwned {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ChangeType {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub id: String,
    pub timestamp: i64,
    pub author: String,
    pub bundle_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Change {
    pub id: String,
    pub transaction_id: String,
    pub entity_type: String,
    pub entity_key: String,
    pub change_type: String,
    pub old_values: Option<String>,
    pub new_values: Option<String>,
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

#[derive(Debug, Clone)]
pub struct QueryChange<T> {
    pub subscription_id: String,
    pub new_result: QueryResult<T>,
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

pub struct QuerySubscriber<T> {
    pub(crate) subscription_id: String,
    pub(crate) db: Weak<RwLock<HashMap<String, QuerySubscription>>>,
    pub(crate) _receiver: Receiver<QueryResult<T>>,
}

impl<T> Drop for QuerySubscriber<T> {
    fn drop(&mut self) {
        if let Some(subscriptions) = self.db.upgrade() {
            if let Ok(mut subs) = subscriptions.write() {
                subs.remove(&self.subscription_id);
            }
        }
    }
}

impl<T> QuerySubscriber<T> {
    pub fn recv(&self) -> Result<QueryResult<T>, std::sync::mpsc::RecvError> {
        self._receiver.recv()
    }

    pub fn recv_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<QueryResult<T>, std::sync::mpsc::RecvTimeoutError> {
        self._receiver.recv_timeout(timeout)
    }

    pub fn try_recv(&self) -> Result<QueryResult<T>, std::sync::mpsc::TryRecvError> {
        self._receiver.try_recv()
    }

    pub fn iter(&self) -> std::sync::mpsc::Iter<QueryResult<T>> {
        self._receiver.iter()
    }
}

