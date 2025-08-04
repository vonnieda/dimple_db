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

/// Remote changelog backed by storage with time-based bucketing for performance
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

impl<'a> Changelog for BatchingStorageChangelog<'a> {
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
    
    fn get_changes_after(&self, after_id: Option<&str>) -> Result<Vec<ChangelogChangeWithFields>> {
        let all_change_ids = self.get_all_change_ids()?;
        
        let filtered_ids: Vec<&String> = if let Some(after) = after_id {
            all_change_ids.iter().filter(|id| id.as_str() > after).collect()
        } else {
            all_change_ids.iter().collect()
        };
        
        let mut changes = Vec::new();
        for change_id in filtered_ids {
            let path = self.prefixed_path(&format!("changes/{}.msgpack", change_id));
            if let Ok(data) = self.storage.get(&path) {
                if let Ok(change) = rmp_serde::from_slice::<ChangelogChangeWithFields>(&data) {
                    changes.push(change);
                }
            }
        }
        
        changes.sort_by(|a, b| a.change.id.cmp(&b.change.id));
        Ok(changes)
    }
    
    fn append_changes(&self, changes: Vec<ChangelogChangeWithFields>) -> Result<()> {
        for change in changes {
            let path = self.prefixed_path(&format!("changes/{}.msgpack", change.change.id));
            let data = rmp_serde::to_vec(&change)?;
            self.storage.put(&path, &data)?;
        }
        Ok(())
    }
    
    fn has_change(&self, change_id: &str) -> Result<bool> {
        let path = self.prefixed_path(&format!("changes/{}.msgpack", change_id));
        Ok(self.storage.get(&path).is_ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{changelog::{Changelog, ChangelogChange, RemoteFieldRecord}, storage::InMemoryStorage};

    fn create_test_change(id: &str, entity_id: &str, name: &str) -> ChangelogChangeWithFields {
        ChangelogChangeWithFields {
            change: ChangelogChange {
                id: id.to_string(),
                author_id: "test_author".to_string(),
                entity_type: "Artist".to_string(),
                entity_id: entity_id.to_string(),
                merged: false,
            },
            fields: vec![
                RemoteFieldRecord {
                    field_name: "name".to_string(),
                    field_value: rmpv::Value::String(name.to_string().into()),
                },
            ],
        }
    }

    #[test]
    fn batching_storage_changelog_get_all_change_ids() -> Result<()> {
        let storage = InMemoryStorage::new();
        let changelog = BatchingStorageChangelog::new(&storage, "test".to_string());
        
        // Initially empty
        let ids = changelog.get_all_change_ids()?;
        assert_eq!(ids.len(), 0);
        
        // Add some changes
        let changes = vec![
            create_test_change("01890000-0000-7000-8000-000000000001", "artist1", "The Beatles"),
            create_test_change("01890000-0000-7000-8000-000000000002", "artist2", "Pink Floyd"),
        ];
        changelog.append_changes(changes)?;
        
        // Should now have change IDs
        let ids = changelog.get_all_change_ids()?;
        assert_eq!(ids.len(), 2);
        
        // IDs should be sorted
        assert!(ids[0] < ids[1]);
        assert_eq!(ids[0], "01890000-0000-7000-8000-000000000001");
        assert_eq!(ids[1], "01890000-0000-7000-8000-000000000002");
        
        Ok(())
    }

    #[test]
    fn batching_storage_changelog_get_changes_after() -> Result<()> {
        let storage = InMemoryStorage::new();
        let changelog = BatchingStorageChangelog::new(&storage, "test".to_string());
        
        let changes = vec![
            create_test_change("01890000-0000-7000-8000-000000000001", "artist1", "The Beatles"),
            create_test_change("01890000-0000-7000-8000-000000000002", "artist2", "Pink Floyd"),
        ];
        changelog.append_changes(changes)?;
        
        // Get all changes
        let all_changes = changelog.get_changes_after(None)?;
        assert_eq!(all_changes.len(), 2);
        
        // Get changes after first one
        let first_change_id = &all_changes[0].change.id;
        let later_changes = changelog.get_changes_after(Some(first_change_id))?;
        assert_eq!(later_changes.len(), 1);
        assert_eq!(later_changes[0].change.id, all_changes[1].change.id);
        
        Ok(())
    }

    #[test]
    fn batching_storage_changelog_append_changes() -> Result<()> {
        let storage = InMemoryStorage::new();
        let changelog = BatchingStorageChangelog::new(&storage, "test".to_string());
        
        let changes = vec![
            create_test_change("01890000-0000-7000-8000-000000000001", "artist1", "The Beatles"),
        ];
        
        // Append the changes
        changelog.append_changes(changes)?;
        
        // Verify they were stored
        let all_changes = changelog.get_all_change_ids()?;
        assert_eq!(all_changes.len(), 1);
        assert_eq!(all_changes[0], "01890000-0000-7000-8000-000000000001");
        
        Ok(())
    }

    #[test]
    fn batching_storage_changelog_has_change() -> Result<()> {
        let storage = InMemoryStorage::new();
        let changelog = BatchingStorageChangelog::new(&storage, "test".to_string());
        
        // Initially should not have any changes
        assert!(!changelog.has_change("01890000-0000-7000-8000-000000000001")?);
        
        // Add a change
        let changes = vec![
            create_test_change("01890000-0000-7000-8000-000000000001", "artist1", "The Beatles"),
        ];
        changelog.append_changes(changes)?;
        
        // Should now have the change
        assert!(changelog.has_change("01890000-0000-7000-8000-000000000001")?);
        assert!(!changelog.has_change("01890000-0000-7000-8000-000000000002")?);
        
        Ok(())
    }

    #[test]
    fn batching_storage_changelog_bucketing() -> Result<()> {
        let storage = InMemoryStorage::new();
        let changelog = BatchingStorageChangelog::new(&storage, "test".to_string());
        
        // Add changes with UUIDv7s that should go in different buckets
        // Using different timestamps in the UUID to force different buckets
        let changes = vec![
            create_test_change("01890000-0000-7000-8000-000000000001", "artist1", "The Beatles"),
            create_test_change("01899999-0000-7000-8000-000000000002", "artist2", "Pink Floyd"), // Different bucket
        ];
        changelog.append_changes(changes)?;
        
        // Verify both changes are retrievable
        let all_changes = changelog.get_all_change_ids()?;
        assert_eq!(all_changes.len(), 2);
        
        // Verify both are findable
        assert!(changelog.has_change("01890000-0000-7000-8000-000000000001")?);
        assert!(changelog.has_change("01899999-0000-7000-8000-000000000002")?);
        
        Ok(())
    }

    #[test]
    fn batching_storage_changelog_with_prefix() -> Result<()> {
        let storage = InMemoryStorage::new();
        let changelog1 = BatchingStorageChangelog::new(&storage, "user1".to_string());
        let changelog2 = BatchingStorageChangelog::new(&storage, "user2".to_string());
        
        // Add changes to both changelogs
        let changes1 = vec![
            create_test_change("01890000-0000-7000-8000-000000000001", "artist1", "The Beatles"),
        ];
        let changes2 = vec![
            create_test_change("01890000-0000-7000-8000-000000000002", "artist2", "Pink Floyd"),
        ];
        
        changelog1.append_changes(changes1)?;
        changelog2.append_changes(changes2)?;
        
        // Each changelog should only see its own changes
        let ids1 = changelog1.get_all_change_ids()?;
        let ids2 = changelog2.get_all_change_ids()?;
        
        assert_eq!(ids1.len(), 1);
        assert_eq!(ids2.len(), 1);
        assert_eq!(ids1[0], "01890000-0000-7000-8000-000000000001");
        assert_eq!(ids2[0], "01890000-0000-7000-8000-000000000002");
        
        Ok(())
    }

    // Tests for BasicStorageChangelog
    #[test]
    fn basic_storage_changelog_get_all_change_ids() -> Result<()> {
        let storage = InMemoryStorage::new();
        let changelog = BasicStorageChangelog::new(&storage, "test".to_string());
        
        // Initially empty
        let ids = changelog.get_all_change_ids()?;
        assert_eq!(ids.len(), 0);
        
        // Add some changes
        let changes = vec![
            create_test_change("01890000-0000-7000-8000-000000000001", "artist1", "The Beatles"),
            create_test_change("01890000-0000-7000-8000-000000000002", "artist2", "Pink Floyd"),
        ];
        changelog.append_changes(changes)?;
        
        // Should now have change IDs
        let ids = changelog.get_all_change_ids()?;
        assert_eq!(ids.len(), 2);
        
        // IDs should be sorted
        assert!(ids[0] < ids[1]);
        assert_eq!(ids[0], "01890000-0000-7000-8000-000000000001");
        assert_eq!(ids[1], "01890000-0000-7000-8000-000000000002");
        
        Ok(())
    }

    #[test]
    fn basic_storage_changelog_get_changes_after() -> Result<()> {
        let storage = InMemoryStorage::new();
        let changelog = BasicStorageChangelog::new(&storage, "test".to_string());
        
        let changes = vec![
            create_test_change("01890000-0000-7000-8000-000000000001", "artist1", "The Beatles"),
            create_test_change("01890000-0000-7000-8000-000000000002", "artist2", "Pink Floyd"),
        ];
        changelog.append_changes(changes)?;
        
        // Get all changes
        let all_changes = changelog.get_changes_after(None)?;
        assert_eq!(all_changes.len(), 2);
        
        // Get changes after first one
        let first_change_id = &all_changes[0].change.id;
        let later_changes = changelog.get_changes_after(Some(first_change_id))?;
        assert_eq!(later_changes.len(), 1);
        assert_eq!(later_changes[0].change.id, all_changes[1].change.id);
        
        Ok(())
    }

    #[test]
    fn basic_storage_changelog_has_change() -> Result<()> {
        let storage = InMemoryStorage::new();
        let changelog = BasicStorageChangelog::new(&storage, "test".to_string());
        
        // Initially should not have any changes
        assert!(!changelog.has_change("01890000-0000-7000-8000-000000000001")?);
        
        // Add a change
        let changes = vec![
            create_test_change("01890000-0000-7000-8000-000000000001", "artist1", "The Beatles"),
        ];
        changelog.append_changes(changes)?;
        
        // Should now have the change
        assert!(changelog.has_change("01890000-0000-7000-8000-000000000001")?);
        assert!(!changelog.has_change("01890000-0000-7000-8000-000000000002")?);
        
        Ok(())
    }

    #[test]
    fn basic_storage_changelog_with_prefix() -> Result<()> {
        let storage = InMemoryStorage::new();
        let changelog1 = BasicStorageChangelog::new(&storage, "user1".to_string());
        let changelog2 = BasicStorageChangelog::new(&storage, "user2".to_string());
        
        // Add changes to both changelogs
        let changes1 = vec![
            create_test_change("01890000-0000-7000-8000-000000000001", "artist1", "The Beatles"),
        ];
        let changes2 = vec![
            create_test_change("01890000-0000-7000-8000-000000000002", "artist2", "Pink Floyd"),
        ];
        
        changelog1.append_changes(changes1)?;
        changelog2.append_changes(changes2)?;
        
        // Each changelog should only see its own changes
        let ids1 = changelog1.get_all_change_ids()?;
        let ids2 = changelog2.get_all_change_ids()?;
        
        assert_eq!(ids1.len(), 1);
        assert_eq!(ids2.len(), 1);
        assert_eq!(ids1[0], "01890000-0000-7000-8000-000000000001");
        assert_eq!(ids2[0], "01890000-0000-7000-8000-000000000002");
        
        Ok(())
    }
}