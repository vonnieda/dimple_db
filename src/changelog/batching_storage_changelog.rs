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
        
        // Split changes into batches of approximately 100MB
        const MAX_BATCH_SIZE: usize = 100 * 1024 * 1024; // 100MB
        let mut batches = Vec::new();
        let mut current_batch = Vec::new();
        let mut current_batch_size = 0;
        
        for change in new_changes {
            // Estimate the size of this change when serialized
            let change_size = rmp_serde::to_vec(&change)?.len();
            
            // If adding this change would exceed the limit, start a new batch
            if !current_batch.is_empty() && current_batch_size + change_size > MAX_BATCH_SIZE {
                batches.push(current_batch);
                current_batch = Vec::new();
                current_batch_size = 0;
            }
            
            current_batch_size += change_size;
            current_batch.push(change);
        }
        
        // Don't forget the last batch
        if !current_batch.is_empty() {
            batches.push(current_batch);
        }
        
        // Track all batch IDs and their associated changes for manifest updates
        let mut batch_to_changes: Vec<(String, Vec<ChangelogChangeWithFields>)> = Vec::new();
        
        // Write each batch
        for batch_changes in batches {
            let batch_id = Uuid::now_v7().to_string();
            let batch_path = self.prefixed_path(&format!("batches/{}.msgpack", batch_id));
            let batch_data = rmp_serde::to_vec(&batch_changes)?;
            self.storage.put(&batch_path, &batch_data)?;
            
            batch_to_changes.push((batch_id, batch_changes));
        }
        
        // Update author manifests with all the new batch mappings
        let mut author_manifests: HashMap<String, HashMap<String, String>> = HashMap::new();
        
        // First, load all existing manifests for authors we'll be updating
        let mut authors_to_update = HashSet::new();
        for (_, batch_changes) in &batch_to_changes {
            for change in batch_changes {
                authors_to_update.insert(change.change.author_id.clone());
            }
        }
        
        for author_id in &authors_to_update {
            let manifest_path = self.prefixed_path(&format!("manifests/{}.msgpack", author_id));
            let manifest = match self.storage.get(&manifest_path) {
                Ok(data) => rmp_serde::from_slice(&data)?,
                Err(_) => HashMap::new(),
            };
            author_manifests.insert(author_id.clone(), manifest);
        }
        
        // Add new mappings for each batch
        for (batch_id, batch_changes) in batch_to_changes {
            for change in batch_changes {
                let manifest = author_manifests
                    .entry(change.change.author_id.clone())
                    .or_insert_with(HashMap::new);
                manifest.insert(change.change.id.clone(), batch_id.clone());
            }
        }
        
        // Write all updated manifests
        for (author_id, manifest) in author_manifests {
            let manifest_path = self.prefixed_path(&format!("manifests/{}.msgpack", author_id));
            let manifest_data = rmp_serde::to_vec(&manifest)?;
            self.storage.put(&manifest_path, &manifest_data)?;
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{changelog::{ChangelogChange, RemoteFieldRecord}, storage::InMemoryStorage};

    #[test]
    fn test_large_batch_splitting() -> Result<()> {
        let storage = InMemoryStorage::new();
        let changelog = BatchingStorageChangelog::new(&storage, String::new());
        
        // Create a large set of changes that will exceed 100MB when serialized
        let mut changes = Vec::new();
        
        // Create a large field value (1MB)
        let large_value = vec![0u8; 1024 * 1024];
        let large_msgpack_value = rmpv::Value::Binary(large_value);
        
        // Create 150 changes, each with a 1MB field - total ~150MB
        for i in 0..150 {
            let change = ChangelogChangeWithFields {
                change: ChangelogChange {
                    id: format!("change-{:03}", i),
                    author_id: "author-1".to_string(),
                    entity_type: "TestEntity".to_string(),
                    entity_id: format!("entity-{:03}", i),
                    merged: false,
                },
                fields: vec![RemoteFieldRecord {
                    field_name: "large_field".to_string(),
                    field_value: large_msgpack_value.clone(),
                }],
            };
            changes.push(change);
        }
        
        // Append the changes
        changelog.append_changes(changes)?;
        
        // Verify that multiple batches were created
        let batch_files = storage.list("batches/")?;
        assert!(batch_files.len() > 1, "Expected multiple batches but got {}", batch_files.len());
        
        // Verify all changes can be retrieved
        let all_change_ids = changelog.get_all_change_ids()?;
        assert_eq!(all_change_ids.len(), 150);
        
        // Verify we can retrieve all changes
        let retrieved_changes = changelog.get_changes(None, None)?;
        assert_eq!(retrieved_changes.len(), 150);
        
        Ok(())
    }
    
    #[test]
    fn test_small_batch_not_split() -> Result<()> {
        let storage = InMemoryStorage::new();
        let changelog = BatchingStorageChangelog::new(&storage, String::new());
        
        // Create a small set of changes
        let mut changes = Vec::new();
        for i in 0..10 {
            let change = ChangelogChangeWithFields {
                change: ChangelogChange {
                    id: format!("change-{:02}", i),
                    author_id: "author-1".to_string(),
                    entity_type: "TestEntity".to_string(),
                    entity_id: format!("entity-{:02}", i),
                    merged: false,
                },
                fields: vec![RemoteFieldRecord {
                    field_name: "name".to_string(),
                    field_value: rmpv::Value::String(format!("Test {}", i).into()),
                }],
            };
            changes.push(change);
        }
        
        // Append the changes
        changelog.append_changes(changes)?;
        
        // Verify that only one batch was created
        let batch_files = storage.list("batches/")?;
        assert_eq!(batch_files.len(), 1, "Expected single batch but got {}", batch_files.len());
        
        Ok(())
    }
}
