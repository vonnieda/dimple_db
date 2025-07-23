# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DimpleDb is a reactive data store and sync engine for Rust, built on SQLite. It's designed for local-first applications with multi-device sync capabilities. The project is currently in ALPHA stage.

## Development Commands

### Building and Testing
```bash
# Build the project
cargo build

# Run all tests
cargo test

# Run tests with output visible
cargo test -- --nocapture

# Run specific test modules
cargo test sync::sync_engine

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

# Todo GUI example
cd examples/todo-gui && cargo run
```

## Architecture

### Core Structure
- `src/lib.rs` - Main library entry point
- `src/db/` - Database core functionality
  - `core.rs` - Core database operations and reactive queries
  - `query.rs` - Query and subscription management
  - `transaction.rs` - Transaction handling
  - `changelog.rs` - Change tracking system
- `src/sync/` - Sync engine implementation
  - `sync_engine.rs` - Main sync orchestration
  - `storage/` - Storage backend implementations (S3, local, memory, encrypted)

### Key Design Principles
1. **SQLite-based**: All data operations go through SQLite
2. **Reactive by default**: Queries return subscriptions for live updates
3. **CRDT-like sync**: Tables as grow-only lists, columns as last-write-wins registers
4. **Storage agnostic**: Abstract storage interface allows S3, local files, or in-memory
5. **Encryption optional**: Can wrap any storage backend with E2E encryption

### Important Implementation Details
- Uses UUIDv7 for distributed ordering (hybrid logical clocks)
- All database changes are automatically tracked in a changelog
- Sync works by exchanging change files (JSON format)
- Conflict resolution happens at the attribute level (last-write-wins)
- Connection pooling via r2d2 for SQLite connections

### Testing Approach
- Unit tests are embedded in source files using `#[cfg(test)]` modules
- Integration tests cover sync scenarios and storage backends
- Use `RUST_LOG=debug` for detailed test output
- S3 tests require environment variables to be set

## Common Development Tasks

### Adding a New Storage Backend
1. Implement the `SyncStorage` trait in `src/sync/storage/`
2. Add tests following the pattern in existing storage implementations
3. Consider if encryption wrapper support is needed

### Working with Reactive Queries
- Use `db.query()` for one-time queries
- Use `db.subscribe()` for reactive queries that update automatically
- Remember to handle the subscription lifecycle properly

### Debugging Sync Issues
1. Enable debug logging: `RUST_LOG=debug`
2. Check the changelog table for tracked changes
3. Examine sync metadata in the storage backend
4. Use the test utilities for simulating sync scenarios