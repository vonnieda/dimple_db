# Sync Engine

1. All changes to entities are tracked in the **change log**, which is in 
[ZV_CHANGE and associated tables](#sync-schema). 
2. Each **replica** keeps a complete merged copy of the change log.
3. Each change includes a [UUIDv7](https://datatracker.ietf.org/doc/html/draft-peabody-dispatch-new-uuid-format)
which acts as a [hybrid logical clock](https://muratbuffalo.blogspot.com/2014/07/hybrid-logical-clocks.html),
providing a global order across all replicas.
4. During a sync, a replica pushes any changes from the local changelog that
are not on the remote, and pulls any that are on the remote and not local.
5. After a sync, any new local changelogs are merged into the entity tables.

## Changelog Abstraction

The sync engine uses a `Changelog` trait that provides a unified interface for change tracking.
There are multiple implementations:

- **DbChangelog**: Reads/writes changes directly from/to the database tables
- **BasicStorageChangelog**: Simple storage-based changelog (one file per change)
- **BatchingStorageChangelog**: Optimized implementation that batches changes for efficiency

## Conflict Resolution

The Sync Engine treats the database tables as grow only lists and the columns
as last-write-wins registers, giving it the attributes of a CRDT. Deletes can
be handled at the user level with tombstones.

# Sync Storage

The sync engine supports multiple storage implementations through the `SyncStorage` trait:
- S3 and S3-compatible storage (primary target)
- Local filesystem storage
- In-memory storage (for testing)
- Encrypted storage wrapper using age encryption

## Storage Formats

### Basic Storage Format
Each change is stored as an individual file containing a `RemoteChangeRecord` which combines:
- One record from the ZV_CHANGE table (change metadata)
- Associated records from ZV_CHANGE_FIELD table (individual field changes)

### Batching Storage Format (Optimized)
To improve sync performance and reduce memory usage for large datasets, the `BatchingStorageChangelog`
implementation groups changes into batches:

- **Manifests**: Map change IDs to batch IDs (one per author)
- **Batches**: Contain the actual change data (limited to 100MB per batch)
- Automatically splits large sync operations into manageable chunks
- Prevents memory exhaustion when syncing large datasets


## Directory Structure

### Basic Storage Layout
```
storage_root/
└── changes/
    └── {change_uuid}.msgpack
```

### Batching Storage Layout
```
storage_root/
├── manifests/         # Maps change IDs to batch IDs
│   └── {author_id}.msgpack
└── batches/           # Contains batched change data
    └── {batch_uuid}.msgpack
```

Storage paths support multiple protocols:
- `s3://endpoint/bucket/base_path/`
- `file://base_path/`
- `memory://base_path/`


# Sync Schema

These tables are automatically created and maintained alongside the entity tables and are used to store the change log and sync metadata.

```sql
CREATE TABLE IF NOT EXISTS ZV_METADATA (
	key TEXT NOT NULL PRIMARY KEY,
	value TEXT NOT NULL
);

INSERT OR IGNORE INTO ZV_METADATA (key, value) 
	VALUES ('database_uuid', uuid7());

CREATE TABLE IF NOT EXISTS ZV_CHANGE (
	id TEXT NOT NULL PRIMARY KEY,
	author_id TEXT NOT NULL,
	entity_type TEXT NOT NULL,
	entity_id TEXT NOT NULL,
	merged BOOL NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS ZV_CHANGE_FIELD (
	change_id TEXT NOT NULL,
	field_name TEXT NOT NULL,
	field_value BLOB NOT NULL,
	PRIMARY KEY (change_id, field_name),
	FOREIGN KEY (change_id) REFERENCES ZV_CHANGE(id)
);
```

## Data Structures

### Basic Change File Format

Stored in `changes/{change_uuid}.msgpack` using MessagePack binary format:

**Example logical structure** (represented as JSON for readability):
```json
{
  "change": {
    "id": "01982f1e-dedc-7863-941c-fa5584b872c1",
    "author_id": "01982f1e-dedb-75a2-98d9-088958068486",
    "entity_type": "AlbumArtist",
    "entity_id": "01982f1e-dedc-7863-941c-fa4d1d95389c",
    "merged": false
  },
  "fields": [
    {
      "field_name": "album_id",
      "field_value": "01982f1e-dedc-7863-941c-fa255d8d889d"
    },
    {
      "field_name": "artist_id", 
      "field_value": "01982f1e-dedc-7863-941c-fa034bd17890"
    },
    {
      "field_name": "cover_art",
      "field_value": <binary data as MessagePack Binary type>
    }
  ]
}
```

### Batching Format Structures

**Manifest File** (`manifests/{author_id}.msgpack`):
Maps change IDs to their corresponding batch IDs:
```json
{
  "change-001": "batch-uuid-1",
  "change-002": "batch-uuid-1",
  "change-003": "batch-uuid-2",
  ...
}
```

**Batch File** (`batches/{batch_uuid}.msgpack`):
Contains an array of changes (same format as basic change files):
```json
[
  {
    "change": { ... },
    "fields": [ ... ]
  },
  {
    "change": { ... },
    "fields": [ ... ]
  },
  ...
]
```

Batches are automatically split when they exceed 100MB to prevent memory issues during sync operations.
