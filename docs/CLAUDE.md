# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DimpleDb is a reactive data store and sync engine for Rust, built on SQLite. It's designed for local-first applications with multi-device sync capabilities. The project is currently in ALPHA stage.

## Development Commands

### Building and Testing
```bash
# Build the project
cargo build
cargo build --verbose

# Run all tests
cargo test

# Run tests with output visible
cargo test -- --nocapture

# Run specific test modules
cargo test sync::sync_engine
cargo test storage::s3_storage
cargo test changelog::tests

# Run a single test function
cargo test test_sync_between_two_databases -- --nocapture

# Run tests with debug logging
RUST_LOG=debug cargo test test_sync_between_two_databases -- --nocapture
RUST_LOG=info cargo test test_repeated_sync_no_bloat -- --nocapture

# Lint code
cargo clippy

# Format code
cargo fmt
```

### S3 Integration Testing
```bash
# Set environment variables first
export DIMPLE_TEST_S3_ENDPOINT="https://s3.amazonaws.com"
export DIMPLE_TEST_S3_BUCKET="your-test-bucket"
export DIMPLE_TEST_S3_ACCESS_KEY="your-access-key"
export DIMPLE_TEST_S3_SECRET_KEY="your-secret-key"

# Then run S3 tests
cargo test storage::s3_storage -- --nocapture
```

### Running Examples
```bash
# Quick start example
cargo run --example quick_start
RUST_LOG=debug cargo run --example quick_start

# Test sync example
RUST_LOG=debug cargo run --example test_sync
RUST_LOG=info cargo run --example test_sync

# Todo GUI example
cd examples/todo-gui && cargo run
```

## Architecture

### Core Components

**Database Layer (`src/db/`)**
- `core.rs`: Main `Db` struct with r2d2 connection pooling and reactive subscriptions
- `query.rs`: Query execution and subscription management via channels
- `transaction.rs`: Transaction handling with automatic change tracking
- `mod.rs`: Entity trait definition and database event types

**Changelog System (`src/changelog/`)**
- `changelog.rs`: Abstract `Changelog` trait for change tracking
- `db_changelog.rs`: Database-backed changelog using ZV_CHANGE tables
- `basic_storage_changelog.rs`: Simple storage-based changelog
- `batching_storage_changelog.rs`: Optimized batching (up to 100MB batches)

**Sync Engine (`src/sync/`)**
- `sync_engine.rs`: Main sync orchestration using UUIDv7 for distributed ordering
- Implements distributed changelog approach with hybrid logical clocks
- Handles push/pull operations between local and remote changelogs

**Storage Backends (`src/storage/`)**
- `sync_storage.rs`: Abstract `SyncStorage` trait
- `s3_storage.rs`: Amazon S3 and compatible storage
- `local_storage.rs`: Local filesystem storage
- `memory_storage.rs`: In-memory storage for testing
- `encrypted_storage.rs`: Encryption wrapper using age
- `slow_memory_storage.rs`: Throttled storage for testing

### Key Design Principles

1. **SQLite-based**: All data operations go through SQLite with bundled driver
2. **Reactive by default**: Queries return subscriptions for live updates via channels
3. **CRDT-like sync**: Tables as grow-only lists, columns as last-write-wins registers
4. **Storage agnostic**: Abstract storage interface with multiple implementations
5. **Encryption optional**: Can wrap any storage backend with E2E encryption using age
6. **Batching optimization**: Recent addition for efficient sync of large datasets

### Data Flow

```
Application → Db.save() → Transaction → Changelog → Storage
                ↓
         Reactive Subscriptions ← Query System ← SQLite
                ↓
         Sync Engine ← Change Files (.msgpack) ← Remote Storage
```

### Important Implementation Details

- **UUIDv7 ordering**: All changes use UUIDv7 for temporal ordering across replicas
- **Change tracking**: Automatic via ZV_CHANGE and ZV_CHANGE_FIELD tables
- **Sync format**: MessagePack serialization for compact change files
- **Conflict resolution**: Last-write-wins at attribute level using UUIDv7 timestamps
- **Connection pooling**: r2d2 for SQLite connection management
- **Batching**: Recent optimization groups changes into manifests (100MB limit)
- **Error handling**: Consistent use of `anyhow::Result` throughout

### Sync Storage Structure

```
storage_root/
├── changes/           # Individual change files (legacy)
│   └── {uuid}.msgpack
├── manifests/         # Batched changes (new)
│   └── {uuid}.msgpack
└── batches/           # Batch data files
    └── {uuid}.msgpack
```

## Testing Approach

- Unit tests embedded in source files using `#[cfg(test)]` modules
- Integration tests in `src/lib.rs` covering sync scenarios
- Use `RUST_LOG=debug` or `RUST_LOG=info` for detailed test output
- S3 tests require environment variables (see above)
- Test utilities include memory and slow storage backends

## Common Development Tasks

### Adding a New Storage Backend
1. Implement the `SyncStorage` trait in `src/storage/`
2. Handle `list()`, `get()`, `put()`, and `delete()` operations
3. Add tests following existing storage implementation patterns
4. Consider encryption wrapper compatibility

### Working with the Changelog System
- The `Changelog` trait is the abstraction for change tracking
- `DbChangelog` reads/writes from database tables
- Storage changelogs read/write from storage backends
- Batching changelog optimizes for large datasets

### Debugging Sync Issues
1. Enable debug logging: `RUST_LOG=debug cargo test`
2. Check ZV_CHANGE and ZV_CHANGE_FIELD tables for local changes
3. Examine remote storage for change/manifest/batch files
4. Use `test_sync` example to simulate scenarios

### Working with Reactive Queries
- `db.query()` for one-time queries
- `db.query_subscribe()` for reactive queries with callbacks
- Subscriptions automatically trigger on relevant table changes
- Remember to store subscription handles to prevent dropping

## Recent Updates

- **Batching System**: New `BatchingStorageChangelog` groups changes into manifests and batches for efficiency
- **Changelog Refactoring**: Extracted `Changelog` trait for cleaner abstraction
- **100MB Batch Limit**: Optimization for syncing large datasets without memory issues