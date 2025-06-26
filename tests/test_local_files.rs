use dimple_data::db::Db;
use dimple_data::sync::{SyncEngine, SyncConfig};
use serde::{Deserialize, Serialize};
use anyhow::Result;
use std::fs;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Item {
    key: String,
    name: String,
    value: i32,
}

impl Default for Item {
    fn default() -> Self {
        Self {
            key: String::new(),
            name: String::new(),
            value: 0,
        }
    }
}

#[test]
fn test_local_file_sync() -> Result<()> {
    let _ = env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Debug)
        .is_test(true)
        .try_init();

    // Create test directories
    let test_dir = "test_data";
    let db1_path = format!("{}/db1.sqlite", test_dir);
    let db2_path = format!("{}/db2.sqlite", test_dir);
    let sync_path = format!("{}/sync", test_dir);

    // Clean up from previous runs
    let _ = fs::remove_dir_all(test_dir);
    fs::create_dir_all(test_dir)?;

    // Create databases
    let db1 = Db::open(&db1_path)?;
    let db2 = Db::open(&db2_path)?;

    // Set up schema
    let migration = "CREATE TABLE Item (
        key TEXT NOT NULL PRIMARY KEY,
        name TEXT NOT NULL,
        value INTEGER NOT NULL
    );";

    db1.migrate(&[migration])?;
    db2.migrate(&[migration])?;

    // Create sync clients with local storage
    let config = SyncConfig {
        base_path: "test_sync".to_string(),
        ..Default::default()
    };

    let sync_client1 = SyncEngine::new_with_local(config.clone(), &sync_path);
    let sync_client2 = SyncEngine::new_with_local(config, &sync_path);

    // Add item to db1
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    
    let item1 = Item {
        name: format!("item_from_db1_{}", timestamp),
        value: 100,
        ..Default::default()
    };
    let saved_item1 = db1.save(&item1)?;
    println!("Added to db1: {} = {}", saved_item1.name, saved_item1.value);

    // Sync db1
    sync_client1.sync(&db1)?;

    // Add item to db2
    let item2 = Item {
        name: format!("item_from_db2_{}", timestamp + 1),
        value: 200,
        ..Default::default()
    };
    let saved_item2 = db2.save(&item2)?;
    println!("Added to db2: {} = {}", saved_item2.name, saved_item2.value);

    // Sync db2 (pulls from db1 and pushes own changes)
    sync_client2.sync(&db2)?;

    // Sync db1 again (pulls from db2)
    sync_client1.sync(&db1)?;

    // Verify both databases have both items
    let items_in_db1: Vec<Item> = db1.query("SELECT * FROM Item ORDER BY name", &[])?;
    let items_in_db2: Vec<Item> = db2.query("SELECT * FROM Item ORDER BY name", &[])?;

    println!("Items in db1: {}", items_in_db1.len());
    for item in &items_in_db1 {
        println!("  {} = {}", item.name, item.value);
    }

    println!("Items in db2: {}", items_in_db2.len());
    for item in &items_in_db2 {
        println!("  {} = {}", item.name, item.value);
    }

    // Both databases should have both items
    assert_eq!(items_in_db1.len(), items_in_db2.len());
    assert!(items_in_db1.len() >= 2);

    // Check that both items exist in both databases
    let has_item1_in_db1 = items_in_db1.iter().any(|item| item.key == saved_item1.key);
    let has_item2_in_db1 = items_in_db1.iter().any(|item| item.key == saved_item2.key);
    let has_item1_in_db2 = items_in_db2.iter().any(|item| item.key == saved_item1.key);
    let has_item2_in_db2 = items_in_db2.iter().any(|item| item.key == saved_item2.key);

    assert!(has_item1_in_db1, "db1 should have item1");
    assert!(has_item2_in_db1, "db1 should have item2");
    assert!(has_item1_in_db2, "db2 should have item1");
    assert!(has_item2_in_db2, "db2 should have item2");

    println!("Test completed successfully!");
    println!("Database files created at: {} and {}", db1_path, db2_path);
    println!("Sync files created in: {}", sync_path);

    Ok(())
}