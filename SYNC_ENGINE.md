# Sync Engine

1. All changes to entities are tracked in the **change log**, which is in [ZV_CHANGE and associated tables](#sync-schema). 
2. Each **replica** keeps a complete merged copy of the change log.
3. Each change includes a [UUIDv7](https://datatracker.ietf.org/doc/html/draft-peabody-dispatch-new-uuid-format) which acts as a [hybrid logical clock](https://muratbuffalo.blogspot.com/2014/07/hybrid-logical-clocks.html), providing a global order across all replicas.
4. During a sync, a replica **pulls** new changes from other replicas and **pushes** new local changes made since the last sync.
5. The database tables associated with each entity are kept in sync with the change log as changes from other replicas are merged.

## Conflict Resolution

The Sync Engine treats the database tables as grow only lists and the columns
as last-write-wins registers, giving it the attributes of a CRDT. Deletes can
be handled at the user level with tombstones.

# Sync Storage

Each replica stores a metadata file and one or more change files in the storage.

The metadata includes information about the last file and changes uploaded so that other replicas can quickly determine if there are new changes to sync.

The change files contain an array of one or more change records. The filename is a new UUIDv7 generated when the file is created.

## Directory Structure

```
s3://endpoint/bucket/base_path/ or
file://base_path or
memory://base_path
├── replicas/
│   └── {replica_uuid}.json
└── changes/
    └── {replica_uuid}/
        └── {change_uuid}.json
```

## Useful Prefixes

```
replicas = list("replicas/")
replica_changes = list("changes/{replica_uuid}/")
all_changes = list("changes/")
```

# Sync Schema

These tables are automatically created and maintained alongside the entity tables and are used to store the change log and sync metadata.

```
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
	old_values TEXT,
	new_values TEXT
);
```

## Data Structures

### Replica Metadata

Stored in `replicas/{replica_uuid}.json`:
```json
TODO document
```

### Change File Format

Stored in `changes/{replica_uuid}/{change_file_uuid}.json`:
```json
TODO document
```

## Sync Algorithm

TODO document





