# Todo CLI - DimpleDb Sync Demo

A CLI todo application demonstrating DimpleDb's multi-device sync capabilities. This standalone project shows how to build local-first applications that automatically sync across devices using S3-compatible storage.

## Quick Start

```bash
# Navigate to the todo-cli directory
cd examples/todo-cli

# Build the project
cargo build --release

# Run with cargo
cargo run -- --help

# Or use the binary directly
./target/release/todo --help
```

## Features

- **CRUD Operations**: Create, list, and delete todos
- **Multi-device Sync**: Automatic synchronization across devices via S3-compatible storage  
- **Real-time Updates**: Watch mode shows live changes as they happen
- **Encrypted Storage**: Optional end-to-end encryption with passphrases
- **Tombstone Deletion**: Proper distributed deletion using tombstones
- **Smart ID Matching**: Delete todos using unique ID suffixes

## Usage

### Basic Commands

```bash
# Create a new todo
cargo run -- new "Learn DimpleDb"

# List all todos (syncs first)  
cargo run -- list

# Delete a todo by ID suffix
cargo run -- delete a1b2c3d4

# Manual sync
cargo run -- sync

# Watch for changes and sync continuously
cargo run -- watch
```

### Multi-Device Sync Setup

To enable real sync across devices, provide S3 credentials:

```bash
# Using command line arguments
cargo run -- \
  --s3-endpoint https://s3.amazonaws.com \
  --s3-bucket my-sync-bucket \
  --s3-access-key YOUR_ACCESS_KEY \
  --s3-secret-key YOUR_SECRET_KEY \
  --passphrase "secure passphrase" \
  new "This will sync across all devices!"

# Or use environment variables
export S3_ENDPOINT="https://s3.amazonaws.com"
export S3_BUCKET="my-sync-bucket"  
export S3_ACCESS_KEY="YOUR_ACCESS_KEY"
export S3_SECRET_KEY="YOUR_SECRET_KEY"
export PASSPHRASE="secure passphrase"

cargo run -- new "Synced todo!"
```

## Demo: Multi-Device Sync

1. **Device A**: Start watching for changes
   ```bash
   cargo run -- --s3-endpoint ... --passphrase ... watch
   ```

2. **Device B**: Create todos and see them appear on Device A
   ```bash
   cargo run -- --s3-endpoint ... --passphrase ... new "From Device B"
   cargo run -- --s3-endpoint ... --passphrase ... new "Another one!"
   ```

3. **Device A**: Watch todos appear in real-time within 3 seconds

## Storage Options

- **S3 Compatible**: AWS S3, MinIO, DigitalOcean Spaces, Cloudflare R2, etc.
- **Local Demo**: Without S3 credentials, uses in-memory storage for single-session demo
- **End-to-End Encryption**: All data encrypted with age encryption when passphrase provided


## Architecture

This example showcases DimpleDb's key features:

- **SQLite Backend**: Full SQL capabilities with reactive queries
- **Distributed Change Log**: UUIDv7-ordered change tracking
- **Conflict Resolution**: Last-write-wins per attribute  
- **Reactive Updates**: Live UI updates via database subscriptions
- **Tombstones**: Proper distributed deletion semantics

## Development

```bash
# Run tests (if any)
cargo test

# Run with debug logging
RUST_LOG=debug cargo run -- list

# Build release binary
cargo build --release

# The binary will be at ./target/release/todo
./target/release/todo --help
```

## Why This Matters

This example demonstrates the core value proposition of DimpleDb: building **local-first applications** that:

- Work offline by default
- Sync seamlessly across devices  
- Require minimal server infrastructure
- Handle conflicts automatically
- Provide real-time collaborative experiences

Perfect for applications like note-taking, task management, document editing, and any scenario where users expect their data to be available everywhere while maintaining privacy and control.

---

Built with [DimpleDb](../../README.md) - A reactive data store and sync engine for Rust.