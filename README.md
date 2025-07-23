# DimpleDb

DimpleDb is a reactive data store and sync engine for Rust, based on SQLite.

- Plain SQL for queries, schemas, and migrations.
- Simple query-to-struct mapping with serde.
- Automatic change tracking.
- Reactive queries with live updates.
- Multi-author encrypted sync via any S3 compatible endpoint.

DimpleDb is designed for storing and syncing user data across devices in
local-first applications, and is inspired by 
[Apple's Core Data + CloudKit](https://developer.apple.com/documentation/CoreData/NSPersistentCloudKitContainer).

## Status

DimpleDb is **ALPHA** and should not yet be used for production data. Please
[report any issues](https://github.com/vonnieda/dimple_db/issues) you run into.

DimpleDb is a side quest for my main project, a [local first music player called
Dimple](https://github.com/vonnieda/dimple). Dimple needs fast local storage that
syncs and so DimpleDb is born.

## Quick Start

```rust
use dimple_db::{Db, sync::SyncEngine};
use serde::{Serialize, Deserialize};
use rusqlite_migration::{Migrations, M};

// Entities are serde compatible Rust structs
#[derive(Serialize, Deserialize, Default)]
struct Todo {
    id: String,
    text: String,
    completed: bool,
}

// Open or create a database
let db = Db::open("myapp.db")?;

// Migrations via rusqlite_migration
let migrations = Migrations::new(vec![
    M::up("CREATE TABLE Todo (
        id TEXT PRIMARY KEY,
        text TEXT NOT NULL,
        completed BOOLEAN NOT NULL DEFAULT 0
    );"),
]);
db.migrate(&migrations)?;

// Save a todo
let todo = db.save(&Todo {
    text: "Build cool app".to_string(),
    completed: false,
    ..Default::default()
})?;

// Query todos
let todos: Vec<Todo> = db.query("SELECT * FROM Todo WHERE completed = 0", ())?;

// Query subscribe to get updates
let _sub = db.query_subscribe("SELECT * FROM Todo WHERE completed = 0", (), |todos| {
    println!("Todos updated: {:?}", todos);
})?;

// Setup encrypted sync to S3
let sync = SyncEngine::builder()
    .s3("https://s3.amazonaws.com", "bucket", "us-east-1", "KEY", "SECRET")?
    .encrypted("passphrase")
    .build()?;

// Sync with other devices
sync.sync(&db)?;
```

For more, check out [the examples](https://github.com/vonnieda/dimple_db/tree/main/examples)!

## Sync Engine

DimpleDb's sync engine is implemented as a distributed change log ordered by 
[UUIDv7](https://datatracker.ietf.org/doc/html/draft-peabody-dispatch-new-uuid-format). 
It is designed to use simple file storage with no API requirements to avoid
cloud vendor lock-in. 

The primary target is S3 compatible storage. Connectors are also included for
in memory and local file storage. Changes are pushed and pulled in simple JSON
files, which are encrypted with [age](https://github.com/FiloSottile/age). 

Merge conflicts are automatically resolved last-write-wins at the attribute
level.

See more about the design in [SYNC_ENGINE.md](docs/SYNC_ENGINE.md).


## Sponsoring DimpleDb

If you like DimpleDb, and would like to help me keep working on it, please
consider helping with one of the methods below. Open Source is my full time
job and only source of income, and every single bit helps me keep working on 
open source software full time:

- BTC: 3FMNgdEjbVcxVoAUtgFFpzsuccnU9KMuhx
- ETH: 0xf1CE557bE8645dC70e78Cbb601bAF2b3649169A0
- DOGE: DGNKBH3AN4pUnHs9ZNQpC42ABzJG4mVF3t
- Paypal: jason@vonnieda.org
- Github: https://github.com/vonnieda
- Venmo: @Jason-vonNieda-1
- Ko-Fi: https://ko-fi.com/vonnieda
- Buy Me a Coffee: https://www.buymeacoffee.com/vonnieda
- Something missing or wrong? <a href="mailto:jason@vonnieda.org">Let me know!</a>
- Want to sponsor me? <a href="mailto:jason@vonnieda.org">Get in touch!</a>

## Inspiration and Research

### Core Data
- [Core Data + CloudKit](https://developer.apple.com/documentation/CoreData/NSPersistentCloudKitContainer)
- [Tables and Fields of Core Data](https://fatbobman.com/en/posts/tables_and_fields_of_coredata/)
- [Core Data + CloudKit: Basics](https://fatbobman.com/en/posts/coredatawithcloudkit-1/)

### Encryption
- [age is a simple, modern and secure file encryption tool, format, and library](https://github.com/FiloSottile/age)

### S3 Compatible Storage
- [Compare S3 storage pricing across major providers](https://www.s3compare.io/)
- [Non-Amazon S3 Providers](https://github.com/s3fs-fuse/s3fs-fuse/wiki/Non-Amazon-S3)

### Distributed Systems
- [Ordering Distributed Events](https://medium.com/baseds/ordering-distributed-events-29c1dd9d1eff)
- [Hybrid Logical Clocks](https://muratbuffalo.blogspot.com/2014/07/hybrid-logical-clocks.html)
- [Logical Physical Clocks](https://cse.buffalo.edu/tech-reports/2014-04.pdf)
- [UUIDv7: Timestamp ordered UUIDs](https://datatracker.ietf.org/doc/html/draft-peabody-dispatch-new-uuid-format)
- [Merklizing the key/value store for fun and profit](https://joelgustafson.com/posts/2023-05-04/merklizing-the-key-value-store-for-fun-and-profit)
- [Iroh gives you an API for dialing by public key](https://github.com/n0-computer/iroh)
- [Riffle Systems](https://riffle.systems/)
- [Riffle: Reactive Relational State for Local-First Applications](https://dl.acm.org/doi/pdf/10.1145/3586183.3606801)
- [Willow](https://willowprotocol.org)
- [Willow Compared](https://willowprotocol.org/more/compare/index.html#willow_compared)
- [Willow Sideloading](https://willowprotocol.org/specs/sideloading/index.html#sideloading)

### Local First
- [Local First Landscape](https://www.localfirst.fm/landscape)

### Similar Projects
- [RxDB](https://github.com/pubkey/rxdb)
- [Dexie.js](https://github.com/dexie/Dexie.js)
- [WatermelonDB](https://github.com/nozbe/WatermelonDB)
- [Drift](https://github.com/simolus3/drift)
- [Electric SQL](https://github.com/electric-sql/electric)
- [LiveStore](https://github.com/livestorejs/livestore)	
- [TinyBase](https://github.com/tinyplex/tinybase)	
- [Dolthub](https://github.com/dolthub/dolt)


## License
DimpleDb is licensed under the [MIT License](https://mit-license.org/).
