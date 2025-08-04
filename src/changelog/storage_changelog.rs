use std::collections::HashMap;
use anyhow::Result;
use serde::{Serialize, Deserialize};
use uuid::Uuid;

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

/// Remote changelog backed by storage with time-based bucketing
pub struct StorageChangelog<'a> {
    storage: &'a dyn SyncStorage,
    prefix: String,
}

impl<'a> StorageChangelog<'a> {
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
    
    /// Extract time-based bucket ID from a UUIDv7
    /// Uses the upper 32 bits of the timestamp (about 49 days per bucket)
    fn get_bucket_id(uuid_str: &str) -> Result<String> {
        let uuid = Uuid::parse_str(uuid_str)?;
        let bytes = uuid.as_bytes();
        // UUIDv7 has 48-bit timestamp in first 6 bytes
        // Use upper 32 bits for bucketing (bytes 0-3)
        let bucket = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        Ok(format!("{:08x}", bucket))
    }
    
    /// Group changes by their time bucket
    fn group_changes_by_bucket(changes: &[ChangelogChangeWithFields]) -> HashMap<String, Vec<ChangelogChangeWithFields>> {
        let mut buckets = HashMap::new();
        
        for change in changes {
            if let Ok(bucket_id) = Self::get_bucket_id(&change.change.id) {
                buckets.entry(bucket_id)
                    .or_insert_with(Vec::new)
                    .push(change.clone());
            }
        }
        
        buckets
    }
}

impl<'a> Changelog for StorageChangelog<'a> {
    fn get_all_change_ids(&self) -> Result<Vec<String>> {
        let bucket_prefix = self.prefixed_path("buckets/");
        let bucket_files = self.storage.list(&bucket_prefix)?;
        
        let mut all_ids = Vec::new();
        
        for bucket_path in bucket_files {
            if bucket_path.ends_with(".msgpack") {
                let data = self.storage.get(&bucket_path)?;
                let bucket: ChangeBucket = rmp_serde::from_slice(&data)?;
                all_ids.extend(bucket.change_ids);
            }
        }
        
        all_ids.sort();
        Ok(all_ids)
    }
    
    fn get_changes_after(&self, after_id: Option<&str>) -> Result<Vec<ChangelogChangeWithFields>> {
        let bucket_prefix = self.prefixed_path("buckets/");
        let bucket_files = self.storage.list(&bucket_prefix)?;
        
        let mut all_changes = Vec::new();
        
        // If we have an after_id, determine which buckets we need to check
        let min_bucket = if let Some(after) = after_id {
            Self::get_bucket_id(after).ok()
        } else {
            None
        };
        
        for bucket_path in bucket_files {
            if bucket_path.ends_with(".msgpack") {
                // Extract bucket ID from filename
                if let Some(filename) = bucket_path.strip_prefix(&bucket_prefix) {
                    if let Some(bucket_id) = filename.strip_suffix(".msgpack") {
                        // Skip buckets that are definitely before our after_id
                        if let (Some(min), Some(current)) = (&min_bucket, Some(bucket_id)) {
                            if current < min.as_str() {
                                continue;
                            }
                        }
                        
                        let data = self.storage.get(&bucket_path)?;
                        let bucket: ChangeBucket = rmp_serde::from_slice(&data)?;
                        
                        // Filter changes if we have an after_id
                        if let Some(after) = after_id {
                            all_changes.extend(
                                bucket.changes.into_iter()
                                    .filter(|c| c.change.id.as_str() > after)
                            );
                        } else {
                            all_changes.extend(bucket.changes);
                        }
                    }
                }
            }
        }
        
        // Sort by change ID to maintain order
        all_changes.sort_by(|a, b| a.change.id.cmp(&b.change.id));
        Ok(all_changes)
    }
    
    fn append_changes(&self, changes: Vec<ChangelogChangeWithFields>) -> Result<()> {
        if changes.is_empty() {
            return Ok(());
        }
        
        // Group changes by time bucket
        let buckets = Self::group_changes_by_bucket(&changes);
        
        for (bucket_id, bucket_changes) in buckets {
            let bucket_path = self.prefixed_path(&format!("buckets/{}.msgpack", bucket_id));
            
            // Check if bucket already exists
            let existing_bucket = match self.storage.get(&bucket_path) {
                Ok(data) => {
                    let mut bucket: ChangeBucket = rmp_serde::from_slice(&data)?;
                    // Merge new changes
                    bucket.changes.extend(bucket_changes);
                    bucket.change_ids = bucket.changes.iter()
                        .map(|c| c.change.id.clone())
                        .collect();
                    // Re-sort to maintain order
                    bucket.changes.sort_by(|a, b| a.change.id.cmp(&b.change.id));
                    bucket.change_ids.sort();
                    bucket
                }
                Err(_) => {
                    // New bucket
                    let mut bucket = ChangeBucket::new(bucket_changes);
                    bucket.changes.sort_by(|a, b| a.change.id.cmp(&b.change.id));
                    bucket.change_ids.sort();
                    bucket
                }
            };
            
            // Write the bucket back
            let data = rmp_serde::to_vec(&existing_bucket)?;
            self.storage.put(&bucket_path, &data)?;
        }
        
        Ok(())
    }
    
    fn has_change(&self, change_id: &str) -> Result<bool> {
        // Optimize by checking only the relevant bucket
        let bucket_id = Self::get_bucket_id(change_id)?;
        let bucket_path = self.prefixed_path(&format!("buckets/{}.msgpack", bucket_id));
        
        match self.storage.get(&bucket_path) {
            Ok(data) => {
                let bucket: ChangeBucket = rmp_serde::from_slice(&data)?;
                Ok(bucket.change_ids.binary_search(&change_id.to_string()).is_ok())
            }
            Err(_) => Ok(false),
        }
    }
}