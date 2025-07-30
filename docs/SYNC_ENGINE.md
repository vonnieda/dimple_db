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

## Conflict Resolution

The Sync Engine treats the database tables as grow only lists and the columns
as last-write-wins registers, giving it the attributes of a CRDT. Deletes can
be handled at the user level with tombstones.

# Sync Storage

Each replica stores one or more change files in the storage.

Each change file contains a `RemoteChangeRecord` which combines:
- One record from the ZV_CHANGE table (change metadata)
- Associated records from ZV_CHANGE_FIELD table (individual field changes)


## Directory Structure

```
s3://endpoint/bucket/base_path/ or
file://base_path or
memory://base_path
└── changes/
	└── {change_uuid}.msgpack
```


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

### Change File Format

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
