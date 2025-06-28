# DimpleDb

DimpleDb is a reactive SQLite database wrapper for Rust that provides:

- Plain SQL for queries, schemas, and migrations.
- Simple table-to-struct mapping with serde.
- Automatic change tracking and history.
- Reactive queries with live updates.
- Multi-author encrypted sync via any S3 compatible endpoint.

DimpleDb is designed for storing and syncing user data across devices in
local-first applications, and is inspired by Apple's Core Data + CloudKit.

## Project Status

DimpleDb is currently being developed and is not ready for use. Please
leave a star and check back soon!

## Sync Engine

DimpleDb's sync engine is implemented as a distributed change log globally
ordered by UUIDv7. It is designed to use simple file storage with no API
requirements to avoid cloud vendor lock-in. 

The primary target is S3 compatible storage. Connectors are also included for
in memory and local file storage. Changes are pushed and pulled in simple JSON
files, which are optionally encrypted with [age](https://github.com/FiloSottile/age)
passphrase encryption. 

Merge conflicts are automatically resolved last-write-wins at the attribute
level.

## Inspiration and Research

- [Core Data + CloudKit](https://developer.apple.com/documentation/CoreData/NSPersistentCloudKitContainer)
- [Tables and Fields of Core Data](https://fatbobman.com/en/posts/tables_and_fields_of_coredata/)
- [Core Data + CloudKit: Basics](https://fatbobman.com/en/posts/coredatawithcloudkit-1/)
- [age is a simple, modern and secure file encryption tool, format, and library](https://github.com/FiloSottile/age)
- [Compare S3 storage pricing across major providers](https://www.s3compare.io/)
- [Non-Amazon S3 Providers](https://github.com/s3fs-fuse/s3fs-fuse/wiki/Non-Amazon-S3)
- [Iroh gives you an API for dialing by public key](https://github.com/n0-computer/iroh)
- [UUIDv7: Timestamp ordered UUIDs](https://datatracker.ietf.org/doc/html/draft-peabody-dispatch-new-uuid-format)
- [RxDB is a local-first, NoSQL-database for JavaScript Applications](https://github.com/pubkey/rxdb)
- [Dexie.js is a wrapper library for indexedDB](https://github.com/dexie/Dexie.js)
- [WatermelondDB: A reactive database framework](https://github.com/nozbe/WatermelonDB)
- [Drift is a reactive persistence library for Flutter and Dart, built on top of SQLite](https://github.com/simolus3/drift)
- [Electric SQL: Real-time sync for Postgres.](https://github.com/electric-sql/electric)

