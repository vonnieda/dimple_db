use dimple_data::db::Db;
use dimple_data::sync::{SyncTarget, SyncEngine, SyncConfig, InMemoryStorage, EncryptedStorage};
use serde::{Deserialize, Serialize};
use anyhow::Result;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct SecretData {
    key: String,
    secret: String,
    value: i32,
}

impl Default for SecretData {
    fn default() -> Self {
        Self {
            key: String::new(),
            secret: String::new(),
            value: 0,
        }
    }
}

#[test]
fn test_encrypted_sync_between_clients() -> Result<()> {
    // Initialize debug logging
    let _ = env_logger::Builder::from_default_env()
        .is_test(true)
        .try_init();

    println!("🔐 Testing Encrypted Sync Between Clients");

    // Create two databases
    let alice_db = Db::open_memory()?;
    let bob_db = Db::open_memory()?;

    // Set up schema
    let migration = "CREATE TABLE SecretData (
        key TEXT NOT NULL PRIMARY KEY,
        secret TEXT NOT NULL,
        value INTEGER NOT NULL
    );";

    alice_db.migrate_sql(&[migration])?;
    bob_db.migrate_sql(&[migration])?;

    // Create sync config with encryption passphrase
    let passphrase = "super-secret-passphrase-123!";
    let mut sync_config = SyncConfig::default();
    sync_config.passphrase = Some(passphrase.to_string());

    // Create sync clients with shared encrypted storage
    let shared_storage = InMemoryStorage::new();
    let shared_encrypted_storage = EncryptedStorage::new(
        Box::new(shared_storage.clone()),
        passphrase.to_string(),
    );
    
    let alice_sync = SyncEngine {
        config: sync_config.clone(),
        target: Box::new(shared_encrypted_storage.clone()),
    };

    let bob_sync = SyncEngine {
        config: sync_config,
        target: Box::new(shared_encrypted_storage.clone()),
    };

    println!("✅ Set up Alice and Bob with encrypted sync");

    // Alice creates sensitive data
    let secret1 = SecretData {
        secret: "Alice's API key: sk-1234567890".to_string(),
        value: 42,
        ..Default::default()
    };
    let _saved_secret1 = alice_db.save(&secret1)?;

    let secret2 = SecretData {
        secret: "Alice's password: p@ssw0rd!".to_string(),
        value: 100,
        ..Default::default()
    };
    let _saved_secret2 = alice_db.save(&secret2)?;

    println!("🔒 Alice created {} secret entries", 2);

    // Alice syncs (pushes encrypted data)
    alice_sync.sync(&alice_db)?;
    println!("📤 Alice synced her encrypted data");

    // Verify data is encrypted in storage
    let raw_storage_files = shared_storage.list("")?;
    println!("📁 Raw storage contains {} files", raw_storage_files.len());
    
    // Try to read a file directly from storage - it should be encrypted
    if let Some(file_path) = raw_storage_files.first() {
        let encrypted_content = shared_storage.get(file_path)?;
        // The content should be encrypted binary data
        assert!(!encrypted_content.is_empty(), "Content should not be empty");
        // Check that plaintext secrets are not present in the raw encrypted bytes
        let content_string = String::from_utf8_lossy(&encrypted_content);
        assert!(!content_string.contains("API key"), "Raw content should not contain plaintext secrets");
        assert!(!content_string.contains("password"), "Raw content should not contain plaintext secrets");
        println!("✅ Verified data is encrypted in storage");
    }

    // Bob syncs (pulls and decrypts data)
    bob_sync.sync(&bob_db)?;
    println!("📥 Bob synced and decrypted Alice's data");

    // Verify Bob can read the decrypted data
    let bob_secrets: Vec<SecretData> = bob_db.query(
        "SELECT * FROM SecretData ORDER BY value",
        &[]
    )?;

    assert_eq!(bob_secrets.len(), 2, "Bob should have 2 secrets");
    assert_eq!(bob_secrets[0].secret, secret1.secret, "Bob should decrypt secret1");
    assert_eq!(bob_secrets[1].secret, secret2.secret, "Bob should decrypt secret2");

    println!("✅ Bob successfully decrypted all secrets");

    // Bob adds his own secret
    let bob_secret = SecretData {
        secret: "Bob's database connection: postgres://user:pass@host".to_string(),
        value: 200,
        ..Default::default()
    };
    bob_db.save(&bob_secret)?;

    // Bob syncs his new data
    bob_sync.sync(&bob_db)?;
    println!("📤 Bob synced his encrypted data");

    // Alice syncs to get Bob's data
    alice_sync.sync(&alice_db)?;
    println!("📥 Alice synced and decrypted Bob's data");

    // Verify Alice has all data
    let alice_secrets: Vec<SecretData> = alice_db.query(
        "SELECT * FROM SecretData ORDER BY value",
        &[]
    )?;

    assert_eq!(alice_secrets.len(), 3, "Alice should have 3 secrets total");
    assert_eq!(alice_secrets[2].secret, bob_secret.secret, "Alice should decrypt Bob's secret");

    println!("🎉 Encrypted sync test passed!");
    println!("✨ Both clients can sync encrypted data using the same passphrase");

    Ok(())
}

#[test]
fn test_different_passphrases_cannot_decrypt() -> Result<()> {
    // Initialize debug logging
    let _ = env_logger::Builder::from_default_env()
        .is_test(true)
        .try_init();

    println!("🔐 Testing Different Passphrases Cannot Decrypt");

    // Create two databases
    let alice_db = Db::open_memory()?;
    let mallory_db = Db::open_memory()?;

    // Set up schema
    let migration = "CREATE TABLE SecretData (
        key TEXT NOT NULL PRIMARY KEY,
        secret TEXT NOT NULL,
        value INTEGER NOT NULL
    );";

    alice_db.migrate_sql(&[migration])?;
    mallory_db.migrate_sql(&[migration])?;

    // Create sync configs with different passphrases
    let alice_passphrase = "alice-secret-key";
    let mallory_passphrase = "mallory-wrong-key";

    let mut alice_config = SyncConfig::default();
    alice_config.passphrase = Some(alice_passphrase.to_string());

    let mut mallory_config = SyncConfig::default();
    mallory_config.passphrase = Some(mallory_passphrase.to_string());

    // Create sync clients with shared storage
    let shared_storage = InMemoryStorage::new();
    
    let alice_sync = SyncEngine {
        config: alice_config,
        target: Box::new(EncryptedStorage::new(
            Box::new(shared_storage.clone()),
            alice_passphrase.to_string(),
        )),
    };

    let mallory_sync = SyncEngine {
        config: mallory_config,
        target: Box::new(EncryptedStorage::new(
            Box::new(shared_storage.clone()),
            mallory_passphrase.to_string(),
        )),
    };

    println!("✅ Set up Alice and Mallory with different passphrases");

    // Alice creates sensitive data
    let alice_secret = SecretData {
        secret: "Alice's private key: BEGIN RSA PRIVATE KEY...".to_string(),
        value: 1000,
        ..Default::default()
    };
    alice_db.save(&alice_secret)?;

    // Alice syncs her encrypted data
    alice_sync.sync(&alice_db)?;
    println!("📤 Alice synced her encrypted data");

    // Mallory tries to sync with wrong passphrase
    let sync_result = mallory_sync.sync(&mallory_db);
    
    // The sync might succeed at the protocol level, but Mallory shouldn't be able to decrypt
    if sync_result.is_ok() {
        // Check if Mallory has any decrypted data
        let mallory_secrets: Vec<SecretData> = mallory_db.query(
            "SELECT * FROM SecretData",
            &[]
        )?;
        
        // Mallory should not have successfully decrypted any data
        assert_eq!(mallory_secrets.len(), 0, "Mallory should not decrypt any data with wrong passphrase");
        println!("✅ Mallory could not decrypt data with wrong passphrase");
    } else {
        println!("✅ Mallory's sync failed with wrong passphrase");
    }

    println!("🎉 Different passphrase test passed!");
    println!("✨ Data remains secure with different passphrases");

    Ok(())
}
