# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Build and Test
```bash
# Build the project
cargo build

# Run all tests
cargo test

# Run a specific test
cargo test test_name

# Run tests with logging output
RUST_LOG=debug cargo test -- --nocapture

# Run the music library example
cargo run --example music_library
```

### Code Quality
```bash
# Run clippy linter
cargo clippy

# Format code
cargo fmt

# Generate documentation
cargo doc --open
```

## Architecture Overview

DimpleDb is a reactive SQLite database wrapper with automatic change tracking and encrypted sync. The architecture consists of three main components:

### 1. Database Core (`src/db/`)
- **core.rs**: Manages SQLite connections, migrations, and CRUD operations. All operations automatically track changes.
- **history.rs**: Implements change tracking using `_change` and `_transaction` tables. Uses UUIDv7 for global ordering.
- **query.rs**: Executes SQL queries and deserializes results into Rust types.
- **reactive.rs**: Provides live query subscriptions that automatically update when underlying data changes.
- **types.rs**: Defines core traits like `Entity` and `QueryResult`.

### 2. Sync Engine (`src/sync/`)
- **sync_engine.rs**: Orchestrates push/pull operations, managing incremental sync state.
- **sync_target.rs**: Abstracts storage backends (S3, Local, Memory) with optional encryption.
- Changes are synced as JSON files, optionally encrypted with age passphrase encryption.
- Conflicts are resolved last-write-wins at the attribute level using UUIDv7 timestamps.

### 3. Notifier System (`src/notifier.rs`)
- Channel-based notification system that powers reactive queries.
- Tracks query dependencies and notifies subscribers when relevant tables change.

## Key Implementation Details

### Entity Requirements
All entities must:
- Implement `Serialize` and `DeserializeOwned`
- Have a `key: String` field (auto-generated if empty)
- Table names are derived from struct names (e.g., `Artist` -> `artist` table)

### Change Tracking
- Always enabled, cannot be disabled
- Each database has a unique author UUID
- Changes are tracked at the attribute level (one change record per modified field)
- Each change stores old_value and new_value for the specific attribute
- Transactions group related changes together using UUIDv7 for ordering

### Reactive Queries
```rust
// Example subscription
db.subscribe("SELECT * FROM artist", |artists: Vec<Artist>| {
    // This closure is called whenever artists change
});
```

### Testing Patterns
- Use `create_test_db()` helper for in-memory test databases
- Integration tests in `tests/` cover sync scenarios
- Enable logging with `env_logger::init()` for debugging

## Common Tasks

### Adding a New Entity
1. Define struct with `Serialize`/`Deserialize` and `key` field
2. Create table in migration
3. Use `db.save()` for both insert and update operations
4. Changes are automatically tracked at the attribute level

### Implementing Sync
1. Create a `SyncTarget` (S3, Local, or Memory)
2. Optionally wrap with `EncryptedSyncTarget` for encryption
3. Use `SyncEngine::new()` with the target
4. Call `engine.sync()` to push/pull changes

### Debugging Sync Issues
- Check `_sync_state` table for last sync timestamps
- Use `RUST_LOG=debug` to see detailed sync operations
- Verify change tracking in `_change` and `_transaction` tables