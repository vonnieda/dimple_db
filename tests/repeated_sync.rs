use dimple_db::db::Db;
use dimple_db::sync::{SyncTarget, SyncEngine, SyncConfig};
use serde::{Deserialize, Serialize};

#[test]
fn test_repeated_sync_no_bloat() -> anyhow::Result<()> {
    // Initialize debug logging
    let _ = env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Debug)
        .is_test(true)
        .try_init();

    println!("🔄 Testing Repeated Sync Operations (No Bloat)");

    // Create two databases
    let alice_db = Db::open_memory()?;
    let bob_db = Db::open_memory()?;

    // Simple schema
    let migration = "CREATE TABLE TestItem (
        key TEXT NOT NULL PRIMARY KEY,
        name TEXT NOT NULL,
        value INTEGER NOT NULL
    );";

    alice_db.migrate_sql(&[migration])?;
    bob_db.migrate_sql(&[migration])?;

    // Set up sync with shared storage
    let sync_config = SyncConfig::default();
    let shared_storage = dimple_db::sync::InMemoryStorage::new();

    let alice_sync = SyncEngine {
        config: sync_config.clone(),
        target: Box::new(shared_storage.clone()),
    };

    let bob_sync = SyncEngine {
        config: sync_config,
        target: Box::new(shared_storage.clone()),
    };

    println!("✅ Set up Alice and Bob databases with sync");

    // =====================================================
    // Phase 1: Initial data and sync
    // =====================================================
    println!("\n📊 Phase 1: Initial Data and Sync");

    // Alice creates some data
    for i in 1..=3 {
        let item = TestItem {
            name: format!("Alice Item {}", i),
            value: i * 10,
            ..Default::default()
        };
        alice_db.save(&item)?;
    }

    // Bob creates some data
    for i in 1..=2 {
        let item = TestItem {
            name: format!("Bob Item {}", i),
            value: i * 100,
            ..Default::default()
        };
        bob_db.save(&item)?;
    }

    // Initial sync
    alice_sync.sync(&alice_db)?;
    bob_sync.sync(&bob_db)?;
    alice_sync.sync(&alice_db)?; // Alice gets Bob's data

    // Measure initial state
    let initial_alice_changes = alice_db.get_all_changes()?.len();
    let initial_bob_changes = bob_db.get_all_changes()?.len();
    let initial_storage_files = count_storage_files(&shared_storage);

    println!("📈 Initial measurements:");
    println!("   Alice changes: {}", initial_alice_changes);
    println!("   Bob changes: {}", initial_bob_changes);
    println!("   Storage files: {}", initial_storage_files);

    // Verify both databases have all data
    let alice_items: Vec<TestItem> = alice_db.query("SELECT * FROM TestItem ORDER BY name", &[])?;
    let bob_items: Vec<TestItem> = bob_db.query("SELECT * FROM TestItem ORDER BY name", &[])?;

    assert_eq!(
        alice_items.len(),
        5,
        "Alice should have 5 items after initial sync"
    );
    assert_eq!(
        bob_items.len(),
        5,
        "Bob should have 5 items after initial sync"
    );

    println!(
        "✅ Initial sync completed - both have {} items",
        alice_items.len()
    );

    // =====================================================
    // Phase 2: Repeated sync with no changes
    // =====================================================
    println!("\n🔄 Phase 2: Repeated Sync Operations (No New Data)");

    // Perform many sync operations with no new data
    for round in 1..=10 {
        print!("   Round {}: ", round);

        // Alice syncs
        alice_sync.sync(&alice_db)?;
        print!("A");

        // Bob syncs
        bob_sync.sync(&bob_db)?;
        print!("B");

        // Alice syncs again
        alice_sync.sync(&alice_db)?;
        print!("A");

        // Bob syncs again
        bob_sync.sync(&bob_db)?;
        println!("B ✓");
    }

    // =====================================================
    // Phase 3: Measure final state
    // =====================================================
    println!("\n📊 Phase 3: Final Measurements");

    let final_alice_changes = alice_db.get_all_changes()?.len();
    let final_bob_changes = bob_db.get_all_changes()?.len();
    let final_storage_files = count_storage_files(&shared_storage);

    println!("📈 Final measurements:");
    println!(
        "   Alice changes: {} (was {})",
        final_alice_changes, initial_alice_changes
    );
    println!(
        "   Bob changes: {} (was {})",
        final_bob_changes, initial_bob_changes
    );
    println!(
        "   Storage files: {} (was {})",
        final_storage_files, initial_storage_files
    );

    // Verify data integrity
    let final_alice_items: Vec<TestItem> =
        alice_db.query("SELECT * FROM TestItem ORDER BY name", &[])?;
    let final_bob_items: Vec<TestItem> =
        bob_db.query("SELECT * FROM TestItem ORDER BY name", &[])?;

    println!("📊 Data integrity:");
    println!(
        "   Alice items: {} (was {})",
        final_alice_items.len(),
        alice_items.len()
    );
    println!(
        "   Bob items: {} (was {})",
        final_bob_items.len(),
        bob_items.len()
    );

    // =====================================================
    // Phase 4: Assertions (No Bloat)
    // =====================================================
    println!("\n✅ Phase 4: Bloat Prevention Verification");

    // Change counts should be identical (no duplicate changes)
    assert_eq!(
        final_alice_changes, initial_alice_changes,
        "Alice's change count should not increase from repeated syncs"
    );
    assert_eq!(
        final_bob_changes, initial_bob_changes,
        "Bob's change count should not increase from repeated syncs"
    );

    // Storage files should not increase (no duplicate uploads)
    assert_eq!(
        final_storage_files, initial_storage_files,
        "Storage file count should not increase from repeated syncs"
    );

    // Data should remain identical
    assert_eq!(
        final_alice_items.len(),
        alice_items.len(),
        "Alice's item count should remain the same"
    );
    assert_eq!(
        final_bob_items.len(),
        bob_items.len(),
        "Bob's item count should remain the same"
    );

    // Verify exact data matches
    for (initial, final_item) in alice_items.iter().zip(final_alice_items.iter()) {
        assert_eq!(
            initial.key, final_item.key,
            "Alice's items should be identical"
        );
        assert_eq!(
            initial.name, final_item.name,
            "Alice's items should be identical"
        );
        assert_eq!(
            initial.value, final_item.value,
            "Alice's items should be identical"
        );
    }

    for (initial, final_item) in bob_items.iter().zip(final_bob_items.iter()) {
        assert_eq!(
            initial.key, final_item.key,
            "Bob's items should be identical"
        );
        assert_eq!(
            initial.name, final_item.name,
            "Bob's items should be identical"
        );
        assert_eq!(
            initial.value, final_item.value,
            "Bob's items should be identical"
        );
    }

    println!("🎉 No Bloat Test Passed!");
    println!("✨ Repeated sync operations cause no storage or database bloat");
    println!("✨ Change tracking remains efficient");
    println!("✨ Data integrity maintained");

    Ok(())
}

// Helper struct for the test
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct TestItem {
    key: String,
    name: String,
    value: i32,
}

impl Default for TestItem {
    fn default() -> Self {
        Self {
            key: String::new(),
            name: String::new(),
            value: 0,
        }
    }
}

// Helper function to count files in storage
fn count_storage_files(storage: &dimple_db::sync::InMemoryStorage) -> usize {
    // Since we can't directly access the internal HashMap, we'll use list operation
    match storage.list("") {
        Ok(files) => files.len(),
        Err(_) => 0,
    }
}
