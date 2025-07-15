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

See more about the design in [[SYNC_ENGINE.md]].


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
- [Hybrid Logical Clocks](https://muratbuffalo.blogspot.com/2014/07/hybrid-logical-clocks.html)
- [Logical Physical Clocks](https://cse.buffalo.edu/tech-reports/2014-04.pdf)

## Local First
- [Local First Landscape](https://www.localfirst.fm/landscape)

## Similar Projects
- [RxDB](https://github.com/pubkey/rxdb)
- [Dexie.js](https://github.com/dexie/Dexie.js)
- [WatermelondDB](https://github.com/nozbe/WatermelonDB)
- [Drift](https://github.com/simolus3/drift)
- [Electric SQL](https://github.com/electric-sql/electric)
- [LiveStore](https://livestore.dev/)	
- [TinyBase](https://github.com/tinyplex/tinybase)	

