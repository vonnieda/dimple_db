use dimple_db::db::{Db, QueryResult};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct Artist {
    pub key: String,
    pub name: String,
    pub disambiguation: Option<String>,
}

#[test]
fn quick_start() -> anyhow::Result<()> {
    env_logger::init();
    let db = Db::open_memory()?;

    // Demonstrate schema migration: start with basic table, then add disambiguation column
    let migrations = &[
        // Migration 1: Create basic Artist table
        "CREATE TABLE Artist (
            key  TEXT NOT NULL PRIMARY KEY,
            name TEXT NOT NULL
        );",
        // Migration 2: Add disambiguation column
        "ALTER TABLE Artist ADD COLUMN disambiguation TEXT;",
    ];

    db.migrate_sql(migrations)?;

    // Track results from reactive query
    let results = std::sync::Arc::new(std::sync::Mutex::new(Vec::<QueryResult<Artist>>::new()));
    let results_clone = results.clone();

    // Observe reactive query with callback
    let _observer = db.observe_query::<Artist>(
        "SELECT * FROM Artist WHERE name LIKE ?",
        &[&"Metal%"],
        move |result| {
            if let Ok(mut r) = results_clone.lock() {
                r.push(result);
            }
        },
    )?;

    // Allow time for initial result
    std::thread::sleep(std::time::Duration::from_millis(10));

    // Save artist - triggers reactive notification
    let artist = db.save(&Artist {
        name: "Metallica".to_string(),
        disambiguation: Some("American heavy metal band".to_string()),
        ..Default::default()
    })?;
    assert!(!artist.key.is_empty());
    assert_eq!(
        artist.disambiguation,
        Some("American heavy metal band".to_string())
    );

    // Allow time for reactive update
    std::thread::sleep(std::time::Duration::from_millis(10));

    // Verify we received both initial and updated results
    let r = results.lock().unwrap();
    assert_eq!(r.len(), 2);
    assert_eq!(r[0].data.len(), 0); // Initial empty result
    assert_eq!(r[1].data.len(), 1); // Updated result with Metallica
    assert_eq!(r[1].data[0].name, "Metallica");
    assert_eq!(
        r[1].data[0].disambiguation,
        Some("American heavy metal band".to_string())
    );

    Ok(())
}