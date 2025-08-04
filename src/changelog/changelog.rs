use anyhow::Result;
use crate::changelog::{ChangelogChangeWithFields};

/// Trait representing a changelog that can be synced between devices
pub trait Changelog: Send + Sync {
    /// Get all change IDs in the changelog
    fn get_all_change_ids(&self) -> Result<Vec<String>>;
    
    /// Get changes after a specific change ID (or all if None)
    fn get_changes_after(&self, after_id: Option<&str>) -> Result<Vec<ChangelogChangeWithFields>>;
    
    /// Append new changes to the changelog
    fn append_changes(&self, changes: Vec<ChangelogChangeWithFields>) -> Result<()>;
    
    /// Check if a specific change exists
    fn has_change(&self, change_id: &str) -> Result<bool>;
}

