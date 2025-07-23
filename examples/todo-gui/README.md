# Todo GUI Demo

A Slint-based GUI application demonstrating DimpleDB's real-time synchronization capabilities.

## Features

- Add, toggle, and delete todo items
- Real-time UI updates using DimpleDB's query subscriptions
- Cross-device synchronization
- S3-based sync backend

## Running the Application

### Basic Usage (Local Only)

```bash
cargo run
```

### With Sync URL

Set the `TODO_SYNC_URL` environment variable to enable cross-device synchronization:

```bash
export TODO_SYNC_URL="s3://access_key:secret_key@endpoint/bucket/prefix?region=us-east-1"
cargo run
```

The sync URL format is: `s3://access_key:secret_key@endpoint/bucket/prefix?region=us-east-1`

When a sync URL is provided, the app will:
- Automatically sync changes every 5 seconds
- Allow multiple instances to stay synchronized

### Environment Variables

- `TODO_DB_PATH`: Database file path (default: `todo-gui.db`)
- `TODO_SYNC_URL`: S3 sync configuration (optional)

## Architecture

The app demonstrates several DimpleDB features:
- **Real-time subscriptions**: UI updates automatically when data changes
- **Cross-device sync**: Multiple instances stay synchronized via S3
- **Conflict resolution**: Built-in CRDT support handles concurrent edits