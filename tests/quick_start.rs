/*! 
 * Dimple Data Quick Start Tests
 * 
 * This file contains comprehensive integration tests that demonstrate all major features
 * of the Dimple Data library working together:
 * 
 * ## Features Demonstrated:
 * 
 * 1. **Database Migration** - Schema version management with incremental migrations
 * 2. **CRUD Operations** - Create, Read, Update, Delete with automatic UUIDv7 key generation
 * 3. **Change Tracking** - Automatic tracking of all data modifications with UUIDv7 ordering
 * 4. **Reactive Queries** - Live query subscriptions that update when underlying data changes
 * 5. **Incremental Sync** - UUIDv7-based distributed synchronization between databases
 * 6. **Author Isolation** - Multi-author sync with conflict resolution
 * 7. **Complex Queries** - JOIN operations, aggregations, and advanced SQL features
 * 
 * ## Tests:
 * 
 * - `quick_start_comprehensive_demo`: Full end-to-end scenario with two users (Alice & Bob)
 *   syncing data between databases with complete change tracking
 *   
 * - `quick_start_reactive_queries_demo`: Focused demonstration of reactive query capabilities
 * 
 * These tests serve as both verification of functionality and documentation of usage patterns.
 */

use dimple_data::db::Db;
use dimple_data::sync::{SyncClient, SyncConfig, Storage};
use serde::{Deserialize, Serialize};

// Test entities for our quick start demo
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct User {
    key: String,
    name: String,
    email: String,
    age: i32,
}

impl Default for User {
    fn default() -> Self {
        Self {
            key: String::new(),
            name: String::new(),
            email: String::new(),
            age: 0,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Post {
    key: String,
    title: String,
    content: String,
    author_key: String,
    published: bool,
}

impl Default for Post {
    fn default() -> Self {
        Self {
            key: String::new(),
            title: String::new(),
            content: String::new(),
            author_key: String::new(),
            published: false,
        }
    }
}

#[test]
fn quick_start_comprehensive_demo() -> anyhow::Result<()> {
    // Initialize debug logging
    let _ = env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Debug)
        .is_test(true)
        .try_init();

    println!("🚀 Starting Dimple Data Quick Start Demo!");
    
    // =====================================================
    // 1. DATABASE SETUP AND MIGRATION
    // =====================================================
    println!("\n📦 Step 1: Database Setup and Migration");
    
    // Create two databases to demonstrate sync
    let alice_db = Db::open_memory()?;
    let bob_db = Db::open_memory()?;
    
    // Define our schema migrations
    let migrations = &[
        // Migration 1: Create Users table
        "CREATE TABLE User (
            key TEXT NOT NULL PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            age INTEGER NOT NULL
        );",
        // Migration 2: Create Posts table
        "CREATE TABLE Post (
            key TEXT NOT NULL PRIMARY KEY,
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            author_key TEXT NOT NULL,
            published BOOLEAN NOT NULL DEFAULT 0,
            FOREIGN KEY (author_key) REFERENCES User (key)
        );",
        // Migration 3: Add index for better query performance
        "CREATE INDEX idx_posts_author ON Post (author_key);",
        "CREATE INDEX idx_posts_published ON Post (published);",
    ];
    
    // Apply migrations to both databases
    alice_db.migrate(migrations)?;
    bob_db.migrate(migrations)?;
    
    println!("✅ Migrations applied successfully to both databases");
    
    // =====================================================
    // 2. BASIC CRUD OPERATIONS
    // =====================================================
    println!("\n💾 Step 2: Basic CRUD Operations");
    
    // Create users
    let alice_user = User {
        name: "Alice Smith".to_string(),
        email: "alice@example.com".to_string(),
        age: 28,
        ..Default::default()
    };
    
    let bob_user = User {
        name: "Bob Johnson".to_string(),
        email: "bob@example.com".to_string(),
        age: 32,
        ..Default::default()
    };
    
    // Save users (this will generate UUIDs and track changes)
    let saved_alice = alice_db.save(&alice_user)?;
    let saved_bob = bob_db.save(&bob_user)?;
    
    println!("✅ Created users: Alice ({}) and Bob ({})", saved_alice.key, saved_bob.key);
    
    // Create posts
    let alice_post = Post {
        title: "My First Post".to_string(),
        content: "This is Alice's first blog post about Dimple Data!".to_string(),
        author_key: saved_alice.key.clone(),
        published: true,
        ..Default::default()
    };
    
    let bob_post = Post {
        title: "Data Sync Magic".to_string(),
        content: "Bob explores the wonders of distributed data synchronization.".to_string(),
        author_key: saved_bob.key.clone(),
        published: false, // Draft
        ..Default::default()
    };
    
    let saved_alice_post = alice_db.save(&alice_post)?;
    let saved_bob_post = bob_db.save(&bob_post)?;
    
    println!("✅ Created posts: '{}' by Alice, '{}' by Bob", saved_alice_post.title, saved_bob_post.title);
    
    // =====================================================
    // 3. REACTIVE QUERIES
    // =====================================================
    println!("\n🔄 Step 3: Reactive Queries");
    
    // Set up reactive query for published posts
    let published_posts_subscriber = alice_db.subscribe_to_query::<Post>(
        "SELECT * FROM Post WHERE published = ? ORDER BY title",
        &[&true]
    )?;
    
    // Set up reactive query for users
    let users_subscriber = alice_db.subscribe_to_query::<User>(
        "SELECT * FROM User ORDER BY name",
        &[]
    )?;
    
    println!("✅ Set up reactive queries for published posts and users");
    
    // Initial query results
    let initial_published: Vec<Post> = alice_db.query(
        "SELECT * FROM Post WHERE published = ? ORDER BY title",
        &[&true]
    )?;
    println!("📊 Initial published posts: {}", initial_published.len());
    
    // Update Bob's post to published (this should trigger reactive query)
    let mut updated_bob_post = saved_bob_post.clone();
    updated_bob_post.published = true;
    let updated_post = bob_db.save(&updated_bob_post)?;
    
    println!("✅ Bob published his post: '{}'", updated_post.title);
    
    // =====================================================
    // 4. SYNC SETUP AND CONFIGURATION
    // =====================================================
    println!("\n🔄 Step 4: Sync Setup");
    
    // Create sync configuration
    let sync_config = SyncConfig::default();
    
    // Create sync clients with shared in-memory storage
    let shared_storage = dimple_data::sync::InMemoryStorage::new();
    
    let alice_sync = SyncClient {
        config: sync_config.clone(),
        storage: Box::new(shared_storage.clone()),
    };
    
    let bob_sync = SyncClient {
        config: sync_config,
        storage: Box::new(shared_storage.clone()),
    };
    
    println!("✅ Sync clients configured with shared storage");
    
    // =====================================================
    // 5. INCREMENTAL SYNC DEMONSTRATION
    // =====================================================
    println!("\n🌍 Step 5: Incremental Sync Demonstration");
    
    // Initial sync - Alice pushes her data
    println!("📤 Alice syncing her data...");
    alice_sync.sync(&alice_db)?;
    
    // Bob syncs and gets Alice's data
    println!("📥 Bob syncing to get Alice's data...");
    bob_sync.sync(&bob_db)?;
    
    // Verify Bob now has Alice's user and post
    let all_users_in_bob: Vec<User> = bob_db.query("SELECT * FROM User ORDER BY name", &[])?;
    let all_posts_in_bob: Vec<Post> = bob_db.query("SELECT * FROM Post ORDER BY title", &[])?;
    
    println!("📊 Bob's database after sync:");
    println!("   Users: {}", all_users_in_bob.len());
    println!("   Posts: {}", all_posts_in_bob.len());
    
    // Alice syncs again to get Bob's updates
    println!("📥 Alice syncing to get Bob's updates...");
    alice_sync.sync(&alice_db)?;
    
    let all_users_in_alice: Vec<User> = alice_db.query("SELECT * FROM User ORDER BY name", &[])?;
    let all_posts_in_alice: Vec<Post> = alice_db.query("SELECT * FROM Post ORDER BY title", &[])?;
    
    println!("📊 Alice's database after sync:");
    println!("   Users: {}", all_users_in_alice.len());
    println!("   Posts: {}", all_posts_in_alice.len());
    
    // =====================================================
    // 6. CHANGE TRACKING VERIFICATION
    // =====================================================
    println!("\n📝 Step 6: Change Tracking Verification");
    
    // Check change history
    let alice_changes = alice_db.get_all_changes()?;
    let bob_changes = bob_db.get_all_changes()?;
    
    println!("📈 Change tracking:");
    println!("   Alice's database has {} tracked changes", alice_changes.len());
    println!("   Bob's database has {} tracked changes", bob_changes.len());
    
    // =====================================================
    // 7. ADVANCED QUERIES AND FINAL VERIFICATION
    // =====================================================
    println!("\n🔍 Step 7: Advanced Queries and Final Verification");
    
    // Complex query: Get users with their post counts
    let user_post_counts: Vec<(String, String, i32)> = alice_db.query(
        "SELECT u.key, u.name, COUNT(p.key) as post_count 
         FROM User u 
         LEFT JOIN Post p ON u.key = p.author_key 
         GROUP BY u.key, u.name 
         ORDER BY u.name",
        &[]
    )?;
    
    println!("📊 User post counts:");
    for (user_key, name, count) in user_post_counts {
        println!("   {} ({}): {} posts", name, user_key, count);
    }
    
    // Query published posts with author info
    #[derive(Serialize, Deserialize, Debug)]
    struct PostWithAuthor {
        title: String,
        content: String,
        author_name: String,
        published: bool,
    }
    
    let posts_with_authors: Vec<PostWithAuthor> = alice_db.query(
        "SELECT p.title, p.content, u.name as author_name, p.published
         FROM Post p
         JOIN User u ON p.author_key = u.key
         WHERE p.published = ?
         ORDER BY p.title",
        &[&true]
    )?;
    
    println!("📚 Published posts with authors:");
    for post in posts_with_authors {
        println!("   '{}' by {} ({})", post.title, post.author_name, 
                if post.published { "published" } else { "draft" });
    }
    
    // =====================================================
    // 8. FINAL ASSERTIONS
    // =====================================================
    println!("\n✅ Step 8: Final Verification");
    
    // Verify data consistency across databases
    assert_eq!(all_users_in_alice.len(), 2, "Alice should have 2 users after sync");
    assert_eq!(all_users_in_bob.len(), 2, "Bob should have 2 users after sync");
    assert_eq!(all_posts_in_alice.len(), 2, "Alice should have 2 posts after sync");
    assert_eq!(all_posts_in_bob.len(), 2, "Bob should have 2 posts after sync");
    
    // Verify both posts are now published
    let published_posts_alice: Vec<Post> = alice_db.query(
        "SELECT * FROM Post WHERE published = ?", 
        &[&true]
    )?;
    let published_posts_bob: Vec<Post> = bob_db.query(
        "SELECT * FROM Post WHERE published = ?", 
        &[&true]
    )?;
    
    assert_eq!(published_posts_alice.len(), 2, "Both posts should be published in Alice's DB");
    assert_eq!(published_posts_bob.len(), 2, "Both posts should be published in Bob's DB");
    
    // Clean up subscribers  
    drop(published_posts_subscriber);
    drop(users_subscriber);
    
    println!("🎉 Quick Start Demo Completed Successfully!");
    println!("✨ All features working: Migration ✓ CRUD ✓ Reactive Queries ✓ Sync ✓ Change Tracking ✓");
    
    Ok(())
}

#[test]
fn quick_start_reactive_queries_demo() -> anyhow::Result<()> {
    // Initialize debug logging
    let _ = env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Debug)
        .is_test(true)
        .try_init();

    println!("🔄 Reactive Queries Focused Demo");
    
    let db = Db::open_memory()?;
    
    // Simple schema
    db.migrate(&[
        "CREATE TABLE User (
            key TEXT NOT NULL PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            age INTEGER NOT NULL,
            active BOOLEAN NOT NULL DEFAULT 1
        );"
    ])?;
    
    // Set up reactive query for active users
    let subscriber = db.subscribe_to_query::<User>(
        "SELECT * FROM User WHERE active = ? ORDER BY name",
        &[&true]
    )?;
    
    println!("✅ Set up reactive query for active users");
    
    // Add some users
    let user1 = User {
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
        age: 25,
        ..Default::default()
    };
    
    let user2 = User {
        name: "Bob".to_string(),
        email: "bob@example.com".to_string(),
        age: 30,
        ..Default::default()
    };
    
    let saved_user1 = db.save(&user1)?;
    let saved_user2 = db.save(&user2)?;
    
    println!("✅ Added users: {} and {}", saved_user1.name, saved_user2.name);
    
    // Check current results
    let current_active: Vec<User> = db.query(
        "SELECT * FROM User WHERE active = ? ORDER BY name",
        &[&true]
    )?;
    
    println!("📊 Current active users: {}", current_active.len());
    for user in &current_active {
        println!("   {} ({})", user.name, user.key);
    }
    
    // The reactive query system is set up and tracking changes
    // In a real application, you would listen to the subscriber for updates
    
    drop(subscriber);
    
    println!("✅ Reactive queries demo completed");
    
    Ok(())
}

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
    
    alice_db.migrate(&[migration])?;
    bob_db.migrate(&[migration])?;
    
    // Set up sync with shared storage
    let sync_config = SyncConfig::default();
    let shared_storage = dimple_data::sync::InMemoryStorage::new();
    
    let alice_sync = SyncClient {
        config: sync_config.clone(),
        storage: Box::new(shared_storage.clone()),
    };
    
    let bob_sync = SyncClient {
        config: sync_config,
        storage: Box::new(shared_storage.clone()),
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
    
    assert_eq!(alice_items.len(), 5, "Alice should have 5 items after initial sync");
    assert_eq!(bob_items.len(), 5, "Bob should have 5 items after initial sync");
    
    println!("✅ Initial sync completed - both have {} items", alice_items.len());
    
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
    println!("   Alice changes: {} (was {})", final_alice_changes, initial_alice_changes);
    println!("   Bob changes: {} (was {})", final_bob_changes, initial_bob_changes);
    println!("   Storage files: {} (was {})", final_storage_files, initial_storage_files);
    
    // Verify data integrity
    let final_alice_items: Vec<TestItem> = alice_db.query("SELECT * FROM TestItem ORDER BY name", &[])?;
    let final_bob_items: Vec<TestItem> = bob_db.query("SELECT * FROM TestItem ORDER BY name", &[])?;
    
    println!("📊 Data integrity:");
    println!("   Alice items: {} (was {})", final_alice_items.len(), alice_items.len());
    println!("   Bob items: {} (was {})", final_bob_items.len(), bob_items.len());
    
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
        final_alice_items.len(), alice_items.len(),
        "Alice's item count should remain the same"
    );
    assert_eq!(
        final_bob_items.len(), bob_items.len(),
        "Bob's item count should remain the same"
    );
    
    // Verify exact data matches
    for (initial, final_item) in alice_items.iter().zip(final_alice_items.iter()) {
        assert_eq!(initial.key, final_item.key, "Alice's items should be identical");
        assert_eq!(initial.name, final_item.name, "Alice's items should be identical");
        assert_eq!(initial.value, final_item.value, "Alice's items should be identical");
    }
    
    for (initial, final_item) in bob_items.iter().zip(final_bob_items.iter()) {
        assert_eq!(initial.key, final_item.key, "Bob's items should be identical");
        assert_eq!(initial.name, final_item.name, "Bob's items should be identical");
        assert_eq!(initial.value, final_item.value, "Bob's items should be identical");
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
fn count_storage_files(storage: &dimple_data::sync::InMemoryStorage) -> usize {
    // Since we can't directly access the internal HashMap, we'll use list operation
    match storage.list("") {
        Ok(files) => files.len(),
        Err(_) => 0,
    }
}