use anyhow::Result;
use rayon::iter::{IntoParallelIterator, ParallelIterator as _};
use uuid::Uuid;

use crate::{changelog::ChangelogChangeWithFields, storage::SyncStorage};
use super::changelog::Changelog;


/// Basic remote changelog backed by storage with one file per change (no batching)
pub struct BasicStorageChangelog<'a> {
    storage: &'a dyn SyncStorage,
    prefix: String,
}

impl<'a> BasicStorageChangelog<'a> {
    pub fn new(storage: &'a dyn SyncStorage, prefix: String) -> Self {
        Self { storage, prefix }
    }
    
    fn prefixed_path(&self, path: &str) -> String {
        if self.prefix.is_empty() {
            path.to_string()
        } else {
            format!("{}/{}", self.prefix, path)
        }
    }
}

impl<'a> Changelog for BasicStorageChangelog<'a> {
    fn get_all_change_ids(&self) -> Result<Vec<String>> {
        let prefix = self.prefixed_path("changes/");
        let files = self.storage.list(&prefix)?;
        
        let mut change_ids = Vec::new();
        for file in files {
            if let Some(path) = file.strip_prefix(&prefix) {
                if let Some(change_id) = path.strip_suffix(".msgpack") {
                    change_ids.push(change_id.to_string());
                }
            }
        }
        
        change_ids.sort();
        Ok(change_ids)
    }
    
    fn get_changes(&self, from_id: Option<&str>, to_id: Option<&str>) -> Result<Vec<ChangelogChangeWithFields>> {
        let from_id = from_id.map(|s| s.to_string()).unwrap_or_else(|| Uuid::nil().to_string());
        let to_id = to_id.map(|s| s.to_string()).unwrap_or_else(|| Uuid::max().to_string());
        let results = self.get_all_change_ids()?
            .into_par_iter()
            .filter(|change_id| change_id >= &from_id && change_id <= &to_id)
            .map(|change_id| {
                let path = self.prefixed_path(&format!("changes/{}.msgpack", change_id));
                let data = self.storage.get(&path)?;
                let change = rmp_serde::from_slice::<ChangelogChangeWithFields>(&data)?;
                Ok(change)
            })
            .collect();
        results
    }

    fn append_changes(&self, changes: Vec<ChangelogChangeWithFields>) -> Result<()> {
        for change in changes {
            let path = self.prefixed_path(&format!("changes/{}.msgpack", change.change.id));
            let data = rmp_serde::to_vec(&change)?;
            self.storage.put(&path, &data)?;
        }
        Ok(())
    }    
}

