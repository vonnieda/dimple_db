# Dimple Data Architecture

## Overview

Dimple Data is a lightweight, reactive SQLite database wrapper for Rust that provides:
- Automatic change tracking and history
- Reactive queries with live updates
- Multi-author sync capabilities
- Encryption support for sync storage

## Core Components

### 1. Database Core (`src/db/core.rs`)
- **Purpose**: Central database connection management and entity operations
- **Key Features**:
  - Connection pooling with RwLock for concurrent access
  - Automatic UUID generation for database identification
  - Schema migration support
  - Generic entity save/update operations with automatic key generation

### 2. Change Tracking (`src/db/changes.rs`)
- **Purpose**: Tracks all database modifications for sync and audit
- **Key Features**:
  - Records changes in `_change` and `_transaction` tables
  - Supports insert, update, and delete operations
  - Maintains change history with UUIDv7 for ordering
  - Enables incremental sync by tracking changes per author

### 3. Query Engine (`src/db/query.rs`)
- **Purpose**: SQL query execution with type-safe deserialization
- **Key Features**:
  - Generic query execution with serde-based row mapping
  - Table dependency extraction via EXPLAIN QUERY PLAN
  - Parameter serialization for reactive queries
  - TODO: Builder pattern for unified query/observe API

### 4. Reactive Queries (`src/db/reactive.rs`)
- **Purpose**: Live query subscriptions that update on data changes
- **Key Features**:
  - Callback-based subscriptions for query results
  - Automatic re-execution when dependent tables change
  - Efficient change detection using result hashing
  - Subscription cleanup on drop

### 5. Sync Engine (`src/sync/`)
- **Purpose**: Multi-author data synchronization
- **Components**:
  - `SyncEngine`: Orchestrates sync operations
  - `SyncTarget` trait: Abstraction for storage backends
  - Storage implementations: S3, Local, Memory, Encrypted
- **Sync Flow**:
  1. Pull changes from other authors since last sync
  2. Apply remote changes to local database
  3. Push new local changes to shared storage

## Data Flow

```
┌─────────────┐     ┌──────────────┐     ┌───────────────┐
│   Entity    │────▶│     Save     │────▶│ Change Track  │
│  (Struct)   │     │   (core.rs)  │     │ (changes.rs)  │
└─────────────┘     └──────────────┘     └───────────────┘
                            │                      │
                            ▼                      ▼
                    ┌──────────────┐     ┌───────────────┐
                    │   SQLite DB  │     │  _change/_tx  │
                    │   (tables)   │     │    tables     │
                    └──────────────┘     └───────────────┘
                            │                      │
                            ▼                      ▼
┌─────────────┐     ┌──────────────┐     ┌───────────────┐
│Query Result │◀────│    Query     │     │     Sync      │
│  Vec<T>     │     │  (query.rs)  │     │(sync_engine.rs)│
└─────────────┘     └──────────────┘     └───────────────┘
```

## Key Design Decisions

### 1. Entity Model
- Entities must implement `Serialize + DeserializeOwned`
- All entities require a `key` field (auto-generated if empty)
- Table names derived from struct names

### 2. Change Tracking
- Always enabled (no opt-out currently)
- Uses UUIDv7 for natural time-based ordering
- Each database instance has a unique UUID as its "author"

### 3. Concurrency Model
- RwLock on SQLite connection (single writer, multiple readers)
- Transactions used for atomic operations
- Reactive queries processed after write lock release

### 4. Sync Architecture
- File-based change bundles (one per author/transaction group)
- Incremental sync using UUID ordering
- Conflict resolution: last-write-wins per entity

## Usage Patterns

### Basic CRUD
```rust
let db = Db::open("mydb.sqlite")?;
let entity = db.save(&MyEntity { name: "test", ..Default::default() })?;
let results: Vec<MyEntity> = db.query("SELECT * FROM MyEntity", &[])?;
```

### Reactive Queries
```rust
let observer = db.observe_query::<MyEntity>(
    "SELECT * FROM MyEntity WHERE active = ?",
    &[&true],
    |result| println!("Data changed: {:?}", result)
)?;
// Drops when observer goes out of scope
```

### Sync
```rust
let sync = SyncEngine::new_with_s3(config)?;
sync.sync(&db)?; // Pull remote changes, push local changes
```