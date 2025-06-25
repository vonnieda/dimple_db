# Dimple Data

Dimple Data is a reactive data store with encrypted sync based on SQLite.

- Plain SQL for queries, schemas, and migrations.
- Subscribe to query results to get updates when data changes.
- Sync via *any* S3 compatible endpoint. No other server support required.
- Sync data can be optionally encrypted with a pass phrase. 

## Quick Start

```rust
use dimple_data::{Db, Entity};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
struct Artist {
    key: String,
    name: String,
    disambiguation: Option<String>,
}

fn main() -> anyhow::Result<()> {
    // Open an in-memory database
    let db = Db::open_memory()?;
    
    // Create your table with plain SQL
    if let Ok(conn) = db.conn.write() {
        conn.execute_batch("
            CREATE TABLE Artist (
                key            TEXT NOT NULL PRIMARY KEY,
                name           TEXT NOT NULL,
                disambiguation TEXT
            );
        ")?;
    }
    
    // Save entities - keys are auto-generated if not provided
    let artist = db.save(&Artist {
        name: "Metallica".to_string(),
        ..Default::default()
    })?;
    
    println!("Saved artist with key: {}", artist.key);
    
    // Query with type safety
    let artists: Vec<Artist> = db.query("SELECT * FROM Artist WHERE name = ?", &[&"Metallica"])?;
    println!("Found {} artists", artists.len());
    
    Ok(())
}
```

## Entity Trait

Any Rust struct that implements `Serialize + DeserializeOwned` automatically becomes an `Entity` that can be stored and retrieved from the database. The `Entity` trait provides the bridge between your Rust types and SQLite tables:

- **Automatic conversion**: Structs are automatically serialized to SQL parameters for storage
- **Type-safe queries**: Query results are deserialized back to your struct types
- **Key generation**: Entities without a `key` field get a UUID v7 automatically assigned
- **Column filtering**: Only struct fields that match database columns are used during save operations

The database uses the struct name as the table name and maps struct fields to table columns by name.

## Dependencies

- **anyhow** - Simplified error handling with context and chaining
- **env_logger** - Environment-configurable logging for development and testing
- **log** - Facade for structured logging throughout the application
- **rusqlite** - Safe SQLite database interface with transaction support and prepared statements
- **serde** - Serialization framework for converting Rust structs to/from various formats
- **serde_json** - JSON serialization support for dynamic entity key generation and manipulation
- **serde_rusqlite** - Bridge between serde and SQLite for automatic struct-to-row conversion
- **uuid** - Generates unique timestamped v7 identifiers for entity keys

