use anyhow::Result;
use crate::changelog::{ChangelogChangeWithFields};

/// Trait representing a changelog that can be synced between devices
pub trait Changelog: Send + Sync {
    /// Get all change IDs in the changelog
    fn get_all_change_ids(&self) -> Result<Vec<String>>;
    
    /// Get all changes between the two change_ids, inclusive. If either is
    /// None the range will be extended to the beginning or end repectively.
    fn get_changes(&self, from_id: Option<&str>, to_id: Option<&str>) -> Result<Vec<ChangelogChangeWithFields>>;
    
    /// Append new changes to the changelog
    fn append_changes(&self, changes: Vec<ChangelogChangeWithFields>) -> Result<()>;
}
