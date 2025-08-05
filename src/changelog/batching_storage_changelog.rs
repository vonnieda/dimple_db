use anyhow::Result;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use crate::{changelog::ChangelogChangeWithFields, storage::SyncStorage};
use super::changelog::Changelog;

pub struct BatchingStorageChangelog<'a> {
    storage: &'a dyn SyncStorage,
    prefix: String,
}

impl<'a> BatchingStorageChangelog<'a> {
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

impl<'a> Changelog for BatchingStorageChangelog<'a> {
    /// Read the manifests, return the change_ids. 
    fn get_all_change_ids(&self) -> Result<Vec<String>> {
        let manifest_prefix = self.prefixed_path("manifests/");
        let manifest_files = self.storage.list(&manifest_prefix)?;
        
        let mut all_change_ids = HashSet::new();
        
        for manifest_path in manifest_files {
            // Skip if not a .msgpack file
            if !manifest_path.ends_with(".msgpack") {
                continue;
            }
            
            let data = self.storage.get(&manifest_path)?;
            let manifest: HashMap<String, String> = rmp_serde::from_slice(&data)?;
            
            // Add all change_ids from this manifest
            for change_id in manifest.keys() {
                all_change_ids.insert(change_id.clone());
            }
        }
        
        let mut sorted_ids: Vec<String> = all_change_ids.into_iter().collect();
        sorted_ids.sort();
        Ok(sorted_ids)
    }

    /// Read the manifests, determine which batches contain the range of changes,
    /// read the batches, return the changes.
    fn get_changes(&self, from_id: Option<&str>, to_id: Option<&str>) -> Result<Vec<ChangelogChangeWithFields>> {
        let from_id = from_id.map(|s| s.to_string()).unwrap_or_else(|| Uuid::nil().to_string());
        let to_id = to_id.map(|s| s.to_string()).unwrap_or_else(|| Uuid::max().to_string());
        
        // First, read all manifests to find which batches we need
        let manifest_prefix = self.prefixed_path("manifests/");
        let manifest_files = self.storage.list(&manifest_prefix)?;
        
        let mut batch_ids_to_fetch = HashSet::new();
        let mut change_id_to_batch: HashMap<String, String> = HashMap::new();
        
        for manifest_path in manifest_files {
            if !manifest_path.ends_with(".msgpack") {
                continue;
            }
            
            let data = self.storage.get(&manifest_path)?;
            let manifest: HashMap<String, String> = rmp_serde::from_slice(&data)?;
            
            // Find change_ids in range and their batch_ids
            for (change_id, batch_id) in manifest {
                if change_id >= from_id && change_id <= to_id {
                    batch_ids_to_fetch.insert(batch_id.clone());
                    change_id_to_batch.insert(change_id, batch_id);
                }
            }
        }
        
        // Now fetch the batches and collect relevant changes
        let mut all_changes = Vec::new();
        
        for batch_id in batch_ids_to_fetch {
            let batch_path = self.prefixed_path(&format!("batches/{}.msgpack", batch_id));
            let data = self.storage.get(&batch_path)?;
            let batch_changes: Vec<ChangelogChangeWithFields> = rmp_serde::from_slice(&data)?;
            
            // Filter to only include changes in the requested range
            for change in batch_changes {
                if change.change.id >= from_id && change.change.id <= to_id {
                    all_changes.push(change);
                }
            }
        }
        
        // Sort by change_id
        all_changes.sort_by(|a, b| a.change.id.cmp(&b.change.id));
        
        Ok(all_changes)
    }

    /// Read the manifests, filter out any changes already stored by id.
    /// Create a new batch on the storage with the filtered changes.
    /// Update each author manifest affected by the new changes.
    /// /batches/[batch_UUIDv7].msgpack
    /// /manifests/[author_id].msgpack
    fn append_changes(&self, changes: Vec<ChangelogChangeWithFields>) -> Result<()> {
        if changes.is_empty() {
            return Ok(());
        }
        
        // Get all existing change_ids to filter out duplicates
        let existing_ids: HashSet<String> = self.get_all_change_ids()?.into_iter().collect();
        
        // Filter out changes that already exist
        let new_changes: Vec<ChangelogChangeWithFields> = changes
            .into_iter()
            .filter(|change| !existing_ids.contains(&change.change.id))
            .collect();
        
        if new_changes.is_empty() {
            return Ok(());
        }
        
        // Generate a new batch ID using UUIDv7
        let batch_id = Uuid::now_v7().to_string();
        
        // Write the batch file
        let batch_path = self.prefixed_path(&format!("batches/{}.msgpack", batch_id));
        let batch_data = rmp_serde::to_vec(&new_changes)?;
        self.storage.put(&batch_path, &batch_data)?;
        
        // Group changes by author_id
        let mut changes_by_author: HashMap<String, Vec<String>> = HashMap::new();
        for change in &new_changes {
            changes_by_author
                .entry(change.change.author_id.clone())
                .or_insert_with(Vec::new)
                .push(change.change.id.clone());
        }
        
        // Update each author's manifest
        for (author_id, change_ids) in changes_by_author {
            let manifest_path = self.prefixed_path(&format!("manifests/{}.msgpack", author_id));
            
            // Load existing manifest or create new one
            let mut manifest: HashMap<String, String> = match self.storage.get(&manifest_path) {
                Ok(data) => rmp_serde::from_slice(&data)?,
                Err(_) => HashMap::new(),
            };
            
            // Add new mappings
            for change_id in change_ids {
                manifest.insert(change_id, batch_id.clone());
            }
            
            // Write updated manifest
            let manifest_data = rmp_serde::to_vec(&manifest)?;
            self.storage.put(&manifest_path, &manifest_data)?;
        }
        
        Ok(())
    }
}
