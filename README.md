# Dimple Data

Reactive data store with S3 compatible synchronization based on SQLite.

Designed for storing and syncing user data across devices in local-first
applications. Inspired by Apple's Core Data + CloudKit.


## Features

- Plain SQLite flavored SQL for queries, schemas, and migrations.
- CRUD operations automatically track changes.
- Subscribe to query results to get updates when data changes.
- Sync via *any* S3 compatible endpoint with optional passphrase encryption.


## Inspiration

- [Core Data + CloudKit](https://developer.apple.com/documentation/CoreData/NSPersistentCloudKitContainer) - Apple's sync solution
- [Core Data Tables and Fields](https://fatbobman.com/en/posts/tables_and_fields_of_coredata/) - Core Data implementation patterns
- [Core Data with CloudKit](https://fatbobman.com/en/posts/coredatawithcloudkit-1/) - CloudKit synchronization guide
- [Drift (Flutter)](https://github.com/simolus3/drift) - Similar local-first database
- [rust-s3](https://github.com/durch/rust-s3) - S3 client library
- [age](https://github.com/FiloSottile/age) - Modern encryption tool
- [S3 Compatible Services](https://www.s3compare.io/) - Comparison of S3-compatible storage
- [Non-Amazon S3 Services](https://github.com/s3fs-fuse/s3fs-fuse/wiki/Non-Amazon-S3) - Alternative S3 implementations
- [Iroh](https://github.com/n0-computer/iroh) - Distributed systems toolkit
- [UUIDv7 Specification](https://datatracker.ietf.org/doc/html/draft-peabody-dispatch-new-uuid-format) - Timestamp-ordered UUIDs

