use std::time::Duration;

use dimple_db::{sync::SyncEngine, Db};
use rusqlite_migration::{Migrations, M};
use serde::{Deserialize, Serialize};

fn main() -> anyhow::Result<()> {
    let migrations = Migrations::new(vec![
        M::up("
            CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL);
            CREATE TABLE Album (id TEXT PRIMARY KEY, title TEXT NOT NULL);
            CREATE TABLE AlbumArtist (
                id TEXT PRIMARY KEY, 
                album_id TEXT NOT NULL, 
                artist_id TEXT NOT NULL, 
                FOREIGN KEY (album_id) REFERENCES Album(id),
                FOREIGN KEY (artist_id) REFERENCES Artist(id)
            );"),
        M::up("ALTER TABLE Artist ADD COLUMN summary TEXT;"),
    ]);

    let db1 = Db::open_memory()?;
    db1.migrate(&migrations)?;
    let sql = "SELECT Album.* FROM Album 
        JOIN Album_Artist ON (Album_Artist.album_id = Album.id)
        JOIN Artist ON (Album_Artist.artist_id = Artist.id)
        WHERE Artist.name = ?";
    db1.query(sql, ("Metallica",)).subscribe(|albums: Vec<Album>| {
        dbg!(albums);
    })?;

    let db2 = Db::open_memory()?;
    db2.migrate(&migrations)?;
    db2.transaction(|txn| {
        let artist = txn.save(&Artist {
            name: "Metallica".to_string(),
            ..Default::default()
        })?;
        // let album = txn.save(Album {
        //     title: "...And Justice For All".to_string(),
        //     ..Default::default()
        // })?;
        // txn.save(AlbumArtist {
        //     album_id: album.id,
        //     artist_id: artist.id,
        //     ..Default::default()
        // })?;
    })?;

    let sync = SyncEngine::builder()
        .in_memory()
        .encrypted("correct horse battery staple")
        .build()?;
    sync.sync(&db1)?;
    sync.sync(&db2)?;
    sync.sync(&db1)?;

    std::thread::sleep(Duration::from_millis(100));
    Ok(())
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct Artist {
    pub id: String,
    pub name: String,
    pub summary: Option<String>,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct Album {
    pub id: String,
    pub title: String,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct AlbumArtist {
    pub id: String,
    pub album_id: String,
    pub artist_id: String,
}


