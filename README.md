# DimpleDb

DimpleDb is a reactive SQLite database wrapper for Rust that provides:
DimpleDb is a reactive data store and sync engine for Rust.

- Plain SQL for queries, schemas, and migrations.
- Simple query-to-struct mapping with serde.
- Automatic history tracking.
- Reactive queries with live updates.
- Multi-author encrypted sync via any S3 compatible endpoint.

DimpleDb is designed for storing and syncing user data across devices in
local-first applications, and is inspired by 
[Apple's Core Data + CloudKit](https://developer.apple.com/documentation/CoreData/NSPersistentCloudKitContainer).

# Status

DimpleDb is currently being developed and is not yet ready for use. Please
follow and star to be notified of new releases.


# Sync Engine

DimpleDb's sync engine is implemented as a distributed change log ordered by 
[UUIDv7](https://datatracker.ietf.org/doc/html/draft-peabody-dispatch-new-uuid-format). 
It is designed to use simple file storage with no API requirements to avoid
cloud vendor lock-in. 

The primary target is S3 compatible storage. Connectors are also included for
in memory and local file storage. Changes are pushed and pulled in simple JSON
files, which are encrypted with [age](https://github.com/FiloSottile/age). 

Merge conflicts are automatically resolved last-write-wins at the attribute
level.

## Storage Layout

Each author / device participating in the sync has a unique UUIDv7 called
their author_id.

Each author / device uploads change transactions in bundles under their
author UUID. Transaction bundles are JSON files containing an array of
transactions including their change records.

Bundles contain an arbitrary number of transactions. When the database is
synced we look at the manifest file dimple_db.json to determine the latest
transaction synced, and then create a new bundle with all transactions
since then.

```
bucket/base_path
├── {author_1_id}/
│   ├── dimple_db.json # Manifest file, updated on every sync
│   ├── {transactions_uuid_v7_uuid_v7}.json # Transaction bundles
│   ├── {transactions_uuid_v7_uuid_v7}.json
│   └── ...
├── {author_2_id}/
│   ├── dimple_db.json
│   ├── {transactions_uuid_v7_uuid_v7}.json # Transaction bundles
│   ├── {transactions_uuid_v7_uuid_v7}.json
│   └── ...
└── ...
```

## Author Manifest File Format

The `dimple_db.json` manifest file tracks the sync state for each author/device and provides metadata about available transaction bundles.

```json
{
  "author_id": "01933b2d-1234-7890-abcd-ef1234567890",
  "last_transaction_id": "01933b2e-3f47-7b12-9c8d-1234567890ab",
  "last_sync_timestamp": "2024-07-11T10:30:45.123Z",
  "bundles": [
    {
      "filename": "01933b2a-1111-7111-1111-111111111111_01933b2d-2222-7222-2222-222222222222.json",
      "first_transaction_id": "01933b2a-1111-7111-1111-111111111111",
      "last_transaction_id": "01933b2d-2222-7222-2222-222222222222",
      "transaction_count": 42,
      "created_at": "2024-07-11T09:15:30.456Z"
    },
    {
      "filename": "01933b2d-3333-7333-3333-333333333333_01933b2e-3f47-7b12-9c8d-1234567890ab.json",
      "first_transaction_id": "01933b2d-3333-7333-3333-333333333333", 
      "last_transaction_id": "01933b2e-3f47-7b12-9c8d-1234567890ab",
      "transaction_count": 15,
      "created_at": "2024-07-11T10:30:45.123Z"
    }
  ]
}
```

This manifest enables efficient sync by allowing other devices to quickly determine which transaction bundles they need to fetch based on their last known sync point.

## Transaction Bundle File Format

Each transaction bundle contains one or more transactions which each contain
one or more changes. The files are named with the first and last transaction
id in the bundle.

```json
[
  {
    "id": "01933b2e-3f47-7b12-9c8d-1234567890ab",
    "author": "01933b2d-1234-7890-abcd-ef1234567890",
    "changes": [
      {
        "id": "01933b2e-3f47-7b13-1234-567890abcdef",
        "transaction_id": "01933b2e-3f47-7b12-9c8d-1234567890ab",
        "entity_type": "users",
        "entity_id": "01933b2e-1111-7777-8888-999999999999",
        "attribute": "name",
        "old_value": "John",
        "new_value": "John Doe"
      },
      {
        "id": "01933b2e-3f47-7b14-5678-90abcdef1234",
        "transaction_id": "01933b2e-3f47-7b12-9c8d-1234567890ab",
        "entity_type": "users", 
        "entity_id": "01933b2e-1111-7777-8888-999999999999",
        "attribute": "email",
        "old_value": null,
        "new_value": "john.doe@example.com"
      }
    ]
  }
]
```

## Encryption

Currently filenames are not encrypted, but their contents are. This may
leak that you are actively using DimpleDb to store data and the rough size
of that data, but not the data itself.



# Inspiration and Research

## Core Data
- [Core Data + CloudKit](https://developer.apple.com/documentation/CoreData/NSPersistentCloudKitContainer)
- [Tables and Fields of Core Data](https://fatbobman.com/en/posts/tables_and_fields_of_coredata/)
- [Core Data + CloudKit: Basics](https://fatbobman.com/en/posts/coredatawithcloudkit-1/)

## Encryption
- [age is a simple, modern and secure file encryption tool, format, and library](https://github.com/FiloSottile/age)

## S3 Compatible Storage
- [Compare S3 storage pricing across major providers](https://www.s3compare.io/)
- [Non-Amazon S3 Providers](https://github.com/s3fs-fuse/s3fs-fuse/wiki/Non-Amazon-S3)

## Distributed Systems
- [Ordering Distributed Events](https://medium.com/baseds/ordering-distributed-events-29c1dd9d1eff)
- [Iroh gives you an API for dialing by public key](https://github.com/n0-computer/iroh)
- [UUIDv7: Timestamp ordered UUIDs](https://datatracker.ietf.org/doc/html/draft-peabody-dispatch-new-uuid-format)
- [Riffle Systems](https://riffle.systems/)
- [Riffle: Reactive Relational State for Local-First Applications](https://dl.acm.org/doi/pdf/10.1145/3586183.3606801)
- https://muratbuffalo.blogspot.com/2014/07/hybrid-logical-clocks.html
- https://cse.buffalo.edu/tech-reports/2014-04.pdf

## Local First
- [Local First Landscape](https://www.localfirst.fm/landscape)

## Similar Projects
- [RxDB is a local-first, NoSQL-database for JavaScript Applications](https://github.com/pubkey/rxdb)
- [Dexie.js is a wrapper library for indexedDB](https://github.com/dexie/Dexie.js)
- [WatermelondDB: A reactive database framework](https://github.com/nozbe/WatermelonDB)
- [Drift is a reactive persistence library for Flutter and Dart, built on top of SQLite](https://github.com/simolus3/drift)
- [Electric SQL: Real-time sync for Postgres.](https://github.com/electric-sql/electric)
- [LiveStore: Reactive SQLite for local-first apps](https://livestore.dev/)	
- [TinyBase: The reactive data store for local-first apps](https://github.com/tinyplex/tinybase)	

