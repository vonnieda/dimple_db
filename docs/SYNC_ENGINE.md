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

Each change file contains one record from the ZV_CHANGE table, representing
one or more column updates to an entity.

## Directory Structure

```
s3://endpoint/bucket/base_path/ or
file://base_path or
memory://base_path
└── changes/
	└── {change_uuid}.json
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
	columns_json TEXT NOT NULL
);
```

## Data Structures

### Change File Format

Stored in `changes/{change_file_uuid}.json`:
```json
{
  "id": "01982f1e-dedc-7863-941c-fa5584b872c1",
  "author_id": "01982f1e-dedb-75a2-98d9-088958068486",
  "entity_type": "AlbumArtist",
  "entity_id": "01982f1e-dedc-7863-941c-fa4d1d95389c",
  "columns_json": "{\"album_id\":\"01982f1e-dedc-7863-941c-fa255d8d889d\",\"artist_id\":\"01982f1e-dedc-7863-941c-fa034bd17890\"}",
  "merged": false
}
```

# TODO

- Might be better to just have save() write to the changelog only, and then
we always just perform a merge? So it's Entity -> save() -> changelog -> tables.
This would ensure the changes are always handled in the same exact way whether
it's sync or save.

- I think it might ultimately be better after all to use manifest files, instead
of list. See https://www.backblaze.com/cloud-storage/transaction-pricing
