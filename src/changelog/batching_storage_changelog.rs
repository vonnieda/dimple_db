use anyhow::Result;
use serde::{Serialize, Deserialize};

use crate::{changelog::ChangelogChangeWithFields, storage::SyncStorage};
use super::changelog::Changelog;

#[derive(Debug, Serialize, Deserialize)]
struct ChangeBucket {
    changes: Vec<ChangelogChangeWithFields>,
    change_ids: Vec<String>,
}

impl ChangeBucket {
    fn new(changes: Vec<ChangelogChangeWithFields>) -> Self {
        let change_ids = changes.iter().map(|c| c.change.id.clone()).collect();
        Self { changes, change_ids }
    }
}

/// Remote changelog backed by storage with time-based bucketing for performance
pub struct BatchingStorageChangelog<'a> {
    storage: &'a dyn SyncStorage,
    prefix: String,
}

impl<'a> BatchingStorageChangelog<'a> {
    pub fn new(storage: &'a dyn SyncStorage, prefix: String) -> Self {
        Self { storage, prefix }
    }
}

impl<'a> Changelog for BatchingStorageChangelog<'a> {
    fn get_all_change_ids(&self) -> Result<Vec<String>> {
        todo!()
    }

    fn get_changes(&self, from_id: Option<&str>, to_id: Option<&str>) -> Result<Vec<ChangelogChangeWithFields>> {
        todo!()
    }

    fn append_changes(&self, changes: Vec<ChangelogChangeWithFields>) -> Result<()> {
        todo!()
    }
}
