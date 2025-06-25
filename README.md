# Dimple Data

Dimple Data is a reactive data store with encrypted S3 sync based on SQLite. It
is inspired by Apple's Core Data + CloudKit and is designed for storing and
syncing user data across devices in local first applications.

- Plain SQL for queries, schemas, and migrations.
- Subscribe to query results to get updates when data changes.
- Sync via *any* S3 compatible endpoint.
- Sync data can be optionally encrypted with a pass phrase.


# References

- https://github.com/simolus3/drift
- https://developer.apple.com/documentation/CoreData/NSPersistentCloudKitContainer
- https://fatbobman.com/en/posts/tables_and_fields_of_coredata/
- https://fatbobman.com/en/posts/coredatawithcloudkit-1/
- https://github.com/FiloSottile/age
- https://www.s3compare.io/
- https://github.com/s3fs-fuse/s3fs-fuse/wiki/Non-Amazon-S3
- https://github.com/n0-computer/iroh