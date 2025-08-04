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
    
    /// Group changes by author and time bucket to avoid concurrency issues
    fn group_changes_by_author_and_bucket(changes: &[ChangelogChangeWithFields]) -> HashMap<(String, String), Vec<ChangelogChangeWithFields>> {
        let mut author_buckets = HashMap::new();
        
        for change in changes {
            if let Ok(bucket_id) = Self::get_bucket_id(&change.change.id) {
                let key = (change.change.author_id.clone(), bucket_id);
                author_buckets.entry(key)
                    .or_insert_with(Vec::new)
                    .push(change.clone());
            }
        }
        
        author_buckets
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
    
    // fn get_changes_after(&self, after_id: Option<&str>) -> Result<Vec<ChangelogChangeWithFields>> {
    //     let bucket_prefix = self.prefixed_path("buckets/");
    //     let bucket_files = self.storage.list(&bucket_prefix)?;
        
    //     let mut all_changes = Vec::new();
        
    //     // If we have an after_id, determine which buckets we need to check
    //     let min_bucket = if let Some(after) = after_id {
    //         Self::get_bucket_id(after).ok()
    //     } else {
    //         None
    //     };
        
    //     for bucket_path in bucket_files {
    //         if bucket_path.ends_with(".msgpack") {
    //             // Extract bucket ID from filename - now it's author_id/bucket_id.msgpack
    //             if let Some(relative_path) = bucket_path.strip_prefix(&bucket_prefix) {
    //                 if let Some(bucket_id) = relative_path.split('/').last().and_then(|f| f.strip_suffix(".msgpack")) {
    //                     // Skip buckets that are definitely before our after_id
    //                     if let (Some(min), Some(current)) = (&min_bucket, Some(bucket_id)) {
    //                         if current < min.as_str() {
    //                             continue;
    //                         }
    //                     }
                        
    //                     let data = self.storage.get(&bucket_path)?;
    //                     let bucket: ChangeBucket = rmp_serde::from_slice(&data)?;
                        
    //                     // Filter changes if we have an after_id
    //                     if let Some(after) = after_id {
    //                         all_changes.extend(
    //                             bucket.changes.into_iter()
    //                                 .filter(|c| c.change.id.as_str() > after)
    //                         );
    //                     } else {
    //                         all_changes.extend(bucket.changes);
    //                     }
    //                 }
    //             }
    //         }
    //     }
        
    //     // Sort by change ID to maintain order
    //     all_changes.sort_by(|a, b| a.change.id.cmp(&b.change.id));
    //     Ok(all_changes)
    // }

    
    
    fn append_changes(&self, changes: Vec<ChangelogChangeWithFields>) -> Result<()> {
        if changes.is_empty() {
            return Ok(());
        }
        
        // Group changes by author and time bucket to avoid concurrency issues
        let author_buckets = Self::group_changes_by_author_and_bucket(&changes);
        
        for ((author_id, bucket_id), bucket_changes) in author_buckets {
            // Use author-specific bucket paths to eliminate race conditions
            let bucket_path = self.prefixed_path(&format!("buckets/{}/{}.msgpack", author_id, bucket_id));
            
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
    
    fn get_changes(&self, start_id: &str, end_id: &str) -> Result<Vec<ChangelogChangeWithFields>> {
        todo!()
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

    // #[test]
    // fn batching_storage_changelog_get_changes_after() -> Result<()> {
    //     let storage = InMemoryStorage::new();
    //     let changelog = BatchingStorageChangelog::new(&storage, "test".to_string());
        
    //     let changes = vec![
    //         create_test_change("01890000-0000-7000-8000-000000000001", "artist1", "The Beatles"),
    //         create_test_change("01890000-0000-7000-8000-000000000002", "artist2", "Pink Floyd"),
    //     ];
    //     changelog.append_changes(changes)?;
        
    //     // Get all changes
    //     let all_changes = changelog.get_changes_after(None)?;
    //     assert_eq!(all_changes.len(), 2);
        
    //     // Get changes after first one
    //     let first_change_id = &all_changes[0].change.id;
    //     let later_changes = changelog.get_changes_after(Some(first_change_id))?;
    //     assert_eq!(later_changes.len(), 1);
    //     assert_eq!(later_changes[0].change.id, all_changes[1].change.id);
        
    //     Ok(())
    // }

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

    // #[test]
    // fn batching_storage_changelog_has_change() -> Result<()> {
    //     let storage = InMemoryStorage::new();
    //     let changelog = BatchingStorageChangelog::new(&storage, "test".to_string());
        
    //     // Initially should not have any changes
    //     assert!(!changelog.has_change("01890000-0000-7000-8000-000000000001")?);
        
    //     // Add a change
    //     let changes = vec![
    //         create_test_change("01890000-0000-7000-8000-000000000001", "artist1", "The Beatles"),
    //     ];
    //     changelog.append_changes(changes)?;
        
    //     // Should now have the change
    //     assert!(changelog.has_change("01890000-0000-7000-8000-000000000001")?);
    //     assert!(!changelog.has_change("01890000-0000-7000-8000-000000000002")?);
        
    //     Ok(())
    // }

    // #[test]
    // fn batching_storage_changelog_bucketing() -> Result<()> {
    //     let storage = InMemoryStorage::new();
    //     let changelog = BatchingStorageChangelog::new(&storage, "test".to_string());
        
    //     // Add changes with UUIDv7s that should go in different buckets
    //     // Using different timestamps in the UUID to force different buckets
    //     let changes = vec![
    //         create_test_change("01890000-0000-7000-8000-000000000001", "artist1", "The Beatles"),
    //         create_test_change("01899999-0000-7000-8000-000000000002", "artist2", "Pink Floyd"), // Different bucket
    //     ];
    //     changelog.append_changes(changes)?;
        
    //     // Verify both changes are retrievable
    //     let all_changes = changelog.get_all_change_ids()?;
    //     assert_eq!(all_changes.len(), 2);
        
    //     // Verify both are findable
    //     assert!(changelog.has_change("01890000-0000-7000-8000-000000000001")?);
    //     assert!(changelog.has_change("01899999-0000-7000-8000-000000000002")?);
        
    //     Ok(())
    // }

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

    // #[test]
    // fn test_concurrent_batching_race_condition() -> Result<()> {
    //     use std::thread;
    //     use std::sync::Arc;
        
    //     let storage = Arc::new(InMemoryStorage::new());
        
    //     // Create changes from two different authors that will end up in the same bucket
    //     // Using the same timestamp prefix to force them into the same bucket
    //     let timestamp_prefix = "01890000-0000-7000-8000";
        
    //     let author1_change = ChangelogChangeWithFields {
    //         change: ChangelogChange {
    //             id: format!("{}-000000000001", timestamp_prefix),
    //             author_id: "author1".to_string(), // Different authors
    //             entity_type: "Artist".to_string(),
    //             entity_id: "artist1".to_string(),
    //             merged: false,
    //         },
    //         fields: vec![
    //             RemoteFieldRecord {
    //                 field_name: "name".to_string(),
    //                 field_value: rmpv::Value::String("Artist by Author 1".to_string().into()),
    //             },
    //         ],
    //     };
        
    //     let author2_change = ChangelogChangeWithFields {
    //         change: ChangelogChange {
    //             id: format!("{}-000000000002", timestamp_prefix),
    //             author_id: "author2".to_string(), // Different authors
    //             entity_type: "Artist".to_string(),
    //             entity_id: "artist2".to_string(),
    //             merged: false,
    //         },
    //         fields: vec![
    //             RemoteFieldRecord {
    //                 field_name: "name".to_string(),
    //                 field_value: rmpv::Value::String("Artist by Author 2".to_string().into()),
    //             },
    //         ],
    //     };
        
    //     // Create two changelogs with the same prefix (simulating two authors syncing to same location)
    //     let storage1 = storage.clone();
    //     let storage2 = storage.clone();
        
    //     // Simulate concurrent append operations using threads
    //     let handle1 = thread::spawn(move || {
    //         let changelog = BatchingStorageChangelog::new(storage1.as_ref(), "shared".to_string());
    //         changelog.append_changes(vec![author1_change])
    //     });
        
    //     let handle2 = thread::spawn(move || {
    //         let changelog = BatchingStorageChangelog::new(storage2.as_ref(), "shared".to_string());
    //         changelog.append_changes(vec![author2_change])
    //     });
        
    //     // Wait for both operations to complete
    //     let result1 = handle1.join().unwrap();
    //     let result2 = handle2.join().unwrap();
        
    //     // Both operations should succeed individually
    //     assert!(result1.is_ok(), "Author 1 append failed: {:?}", result1);
    //     assert!(result2.is_ok(), "Author 2 append failed: {:?}", result2);
        
    //     // Check what actually got stored
    //     let final_changelog = BatchingStorageChangelog::new(storage.as_ref(), "shared".to_string());
    //     let all_changes = final_changelog.get_all_change_ids()?;
        
    //     // With author-specific buckets, both changes should always be preserved
    //     println!("Changes stored: {:?}", all_changes);
    //     println!("Expected 2 changes, got {}", all_changes.len());
        
    //     // This should now always pass with author-specific buckets
    //     assert_eq!(all_changes.len(), 2, "Author-specific buckets should prevent race condition");
        
    //     // Verify both specific changes are present
    //     assert!(all_changes.contains(&format!("{}-000000000001", timestamp_prefix)));
    //     assert!(all_changes.contains(&format!("{}-000000000002", timestamp_prefix)));
        
    //     // Verify we can retrieve both changes individually
    //     assert!(final_changelog.has_change(&format!("{}-000000000001", timestamp_prefix))?);
    //     assert!(final_changelog.has_change(&format!("{}-000000000002", timestamp_prefix))?);
        
    //     Ok(())
    // }

    // #[test]
    // fn test_author_specific_buckets_eliminate_race_condition() -> Result<()> {
    //     let storage = InMemoryStorage::new();
        
    //     // Test multiple authors creating changes in the same time bucket
    //     let timestamp_prefix = "01890000-0000-7000-8000";
        
    //     // Create changes from 3 different authors that will go in the same time bucket
    //     let author1_changes = vec![
    //         create_test_change(&format!("{}-000000000001", timestamp_prefix), "artist1", "Artist 1 by author1"),
    //         create_test_change(&format!("{}-000000000002", timestamp_prefix), "artist2", "Artist 2 by author1"),
    //     ];
        
    //     let author2_changes = vec![
    //         create_test_change(&format!("{}-000000000003", timestamp_prefix), "artist3", "Artist 3 by author2"),
    //     ];
        
    //     let author3_changes = vec![
    //         create_test_change(&format!("{}-000000000004", timestamp_prefix), "artist4", "Artist 4 by author3"),
    //         create_test_change(&format!("{}-000000000005", timestamp_prefix), "artist5", "Artist 5 by author3"),
    //     ];
        
    //     // Manually set the author IDs
    //     let mut author1_changes = author1_changes;
    //     let mut author2_changes = author2_changes;
    //     let mut author3_changes = author3_changes;
        
    //     for change in &mut author1_changes {
    //         change.change.author_id = "author1".to_string();
    //     }
    //     for change in &mut author2_changes {
    //         change.change.author_id = "author2".to_string();
    //     }
    //     for change in &mut author3_changes {
    //         change.change.author_id = "author3".to_string();
    //     }
        
    //     // Create changelogs for each author
    //     let changelog1 = BatchingStorageChangelog::new(&storage, "shared".to_string());
    //     let changelog2 = BatchingStorageChangelog::new(&storage, "shared".to_string());
    //     let changelog3 = BatchingStorageChangelog::new(&storage, "shared".to_string());
        
    //     // Append changes from each author
    //     changelog1.append_changes(author1_changes)?;
    //     changelog2.append_changes(author2_changes)?;
    //     changelog3.append_changes(author3_changes)?;
        
    //     // Verify all changes were preserved
    //     let final_changelog = BatchingStorageChangelog::new(&storage, "shared".to_string());
    //     let all_changes = final_changelog.get_all_change_ids()?;
        
    //     // Should have all 5 changes
    //     assert_eq!(all_changes.len(), 5, "All 5 changes should be preserved with author-specific buckets");
        
    //     // Verify specific changes
    //     let expected_ids = vec![
    //         format!("{}-000000000001", timestamp_prefix),
    //         format!("{}-000000000002", timestamp_prefix),
    //         format!("{}-000000000003", timestamp_prefix),
    //         format!("{}-000000000004", timestamp_prefix),
    //         format!("{}-000000000005", timestamp_prefix),
    //     ];
        
    //     for expected_id in expected_ids {
    //         assert!(all_changes.contains(&expected_id), "Missing change: {}", expected_id);
    //         assert!(final_changelog.has_change(&expected_id)?, "has_change failed for: {}", expected_id);
    //     }
        
    //     Ok(())
    // }
}