# Dimple Data

Dimple Data is a reactive data store with S3 synchronization based on SQLite. 
It is inspired by Apple's Core Data + CloudKit and is designed for storing and 
syncing user data across devices in local-first applications.

## Features

✅ **Database Operations**
- Plain SQL for queries, schemas, and migrations
- Automatic UUIDv7 key generation with embedded timestamps
- CRUD operations with change tracking
- Transaction support with rollback capabilities

✅ **Reactive Queries**  
- Subscribe to query results and get live updates when data changes
- Automatic dependency tracking and query invalidation
- Memory-efficient subscription management with cleanup

✅ **Distributed Synchronization**
- Sync via any S3-compatible endpoint using rust-s3
- UUIDv7-based incremental sync (no clock skew issues)
- Author-based conflict resolution and change attribution  
- Stateless sync supporting multiple endpoints
- Prevention of storage/database bloat from repeated operations
- Optional passphrase-based encryption for synced content

✅ **Storage Abstraction**
- S3Storage for production (AWS S3, MinIO, etc.)
- LocalStorage for file-based testing
- InMemoryStorage for unit testing
- EncryptedStorage wrapper for end-to-end encryption
- Consistent interface across all storage backends

✅ **Change Tracking**
- Automatic tracking of all data modifications
- UUIDv7 ordering ensures correct temporal sequencing
- JSON serialization of old/new values for full audit trail
- Transaction bundling for atomic operations

## Quick Start

```rust
use dimple_data::db::Db;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Default)]
struct User {
    key: String,
    name: String,
    email: String,
    age: i32,
}

// Create database and apply migrations
let db = Db::open_memory()?;
db.migrate(&[
    "CREATE TABLE User (
        key TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL,
        age INTEGER NOT NULL
    )"
])?;

// Create and save entities (UUIDs generated automatically)
let user = User {
    name: "Alice".to_string(),
    email: "alice@example.com".to_string(),
    age: 28,
    ..Default::default()
};
let saved_user = db.save(&user)?;

// Query data
let users: Vec<User> = db.query("SELECT * FROM User WHERE age > ?", &[&25])?;

// Set up reactive queries
let subscriber = db.subscribe_to_query::<User>(
    "SELECT * FROM User WHERE age > ?", 
    &[&25]
)?;

// Sync with S3 (requires configuration)
use dimple_data::sync::{SyncClient, SyncConfig};
let config = SyncConfig {
    bucket: "my-app-data".to_string(),
    access_key: "...".to_string(),
    secret_key: "...".to_string(),
    passphrase: Some("my-encryption-key".to_string()), // Optional encryption
    ..Default::default()
};
let sync_client = SyncClient::new_with_s3(config)?;
sync_client.sync(&db)?;
```

## Architecture

- **Database Layer**: SQLite with automatic change tracking and UUIDv7 keys
- **Reactive Layer**: Query subscriptions with dependency tracking
- **Sync Layer**: UUIDv7-based incremental synchronization 
- **Storage Layer**: Pluggable backends (S3, Local, Memory)
- **Encryption Layer**: Optional end-to-end encryption using age

## Encryption

When a passphrase is provided in `SyncConfig`, Dimple Data automatically encrypts all synced content using the [age](https://github.com/FiloSottile/age) encryption library:

### What Gets Encrypted
- **File Content**: All JSON change bundles are encrypted with age using scrypt-derived keys
- **File Paths**: Stored in plaintext for transparency and simplicity

### How It Works
1. **Key Derivation**: The passphrase is used with age's scrypt key derivation to create encryption keys
2. **Content Encryption**: Each file's content is encrypted using age with a unique random nonce
3. **Storage**: Encrypted binary data is stored directly (no armor/base64 encoding)
4. **Decryption**: Clients with the same passphrase can decrypt and read the data

### Security Properties
- **Forward Secrecy**: Each file uses a unique random nonce
- **Authenticated Encryption**: age provides built-in authentication
- **Key Stretching**: scrypt makes brute-force attacks computationally expensive
- **Cross-Platform**: Works consistently across all supported storage backends

### Usage
```rust
let config = SyncConfig {
    bucket: "my-app-data".to_string(),
    access_key: "...".to_string(),
    secret_key: "...".to_string(),
    passphrase: Some("your-secure-passphrase".to_string()), // Enable encryption
    ..Default::default()
};
```

**Note**: All clients must use the same passphrase to sync encrypted data. Clients with different passphrases cannot decrypt each other's data.

## Testing

The codebase includes comprehensive tests demonstrating all features:

```bash
cargo test
```

See `tests/quick_start.rs` for complete integration examples including migration, CRUD, reactive queries, and sync workflows.

## References

- [Drift (Flutter)](https://github.com/simolus3/drift) - Similar local-first database
- [Core Data + CloudKit](https://developer.apple.com/documentation/CoreData/NSPersistentCloudKitContainer) - Apple's sync solution
- [Core Data Tables and Fields](https://fatbobman.com/en/posts/tables_and_fields_of_coredata/) - Core Data implementation patterns
- [Core Data with CloudKit](https://fatbobman.com/en/posts/coredatawithcloudkit-1/) - CloudKit synchronization guide
- [rust-s3](https://github.com/durch/rust-s3) - S3 client library
- [age](https://github.com/FiloSottile/age) - Modern encryption tool
- [S3 Compatible Services](https://www.s3compare.io/) - Comparison of S3-compatible storage
- [Non-Amazon S3 Services](https://github.com/s3fs-fuse/s3fs-fuse/wiki/Non-Amazon-S3) - Alternative S3 implementations
- [Iroh](https://github.com/n0-computer/iroh) - Distributed systems toolkit
- [UUIDv7 Specification](https://datatracker.ietf.org/doc/html/draft-peabody-dispatch-new-uuid-format) - Timestamp-ordered UUIDs

