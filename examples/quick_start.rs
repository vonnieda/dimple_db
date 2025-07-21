use std::sync::mpsc::channel;

use anyhow::Result;
use dimple_db::{sync::SyncEngine, Db};
use rusqlite_migration::{Migrations, M};
use serde::{Deserialize, Serialize};

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

fn main() -> Result<()> {
    let _ = env_logger::try_init();
    let migrations = Migrations::new(vec![
        M::up("
            CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL);
            CREATE TABLE Album (id TEXT PRIMARY KEY, title TEXT NOT NULL);
            CREATE TABLE Genre (id TEXT PRIMARY KEY, name TEXT NOT NULL);
            CREATE TABLE AlbumArtist (
                id TEXT PRIMARY KEY, 
                album_id TEXT NOT NULL, 
                artist_id TEXT NOT NULL, 
                FOREIGN KEY (album_id) REFERENCES Album(id),
                FOREIGN KEY (artist_id) REFERENCES Artist(id)
            );
            CREATE TABLE ArtistGenre (
                id TEXT PRIMARY KEY, 
                artist_id TEXT NOT NULL, 
                genre_id TEXT NOT NULL, 
                FOREIGN KEY (artist_id) REFERENCES Artist(id),
                FOREIGN KEY (genre_id) REFERENCES Genre(id)
            );
            CREATE TABLE AlbumGenre (
                id TEXT PRIMARY KEY, 
                album_id TEXT NOT NULL, 
                genre_id TEXT NOT NULL, 
                FOREIGN KEY (album_id) REFERENCES Album(id),
                FOREIGN KEY (genre_id) REFERENCES Genre(id)
            );
        "),
        M::up("ALTER TABLE Artist ADD COLUMN summary TEXT;"),
    ]);

    let db1 = Db::open_memory()?;
    db1.migrate(&migrations)?;
    db1.transaction(|txn| {
        let artist = txn.save(&Artist {
            name: "Metallica".to_string(),
            ..Default::default()
        })?;

        let album = txn.save(&Album {
            title: "...And Justice For All".to_string(),
            ..Default::default()
        })?;
        txn.save(&AlbumArtist {
            album_id: album.id,
            artist_id: artist.id,
            ..Default::default()
        })?;
        Ok(())
    })?;

    let db2 = Db::open_memory()?;
    db2.migrate(&migrations)?;
    
    // Set up a reactive query subscription and a way to monitor it
    let sql = "SELECT Album.* FROM Album 
        JOIN AlbumArtist ON (AlbumArtist.album_id = Album.id)
        JOIN Artist ON (AlbumArtist.artist_id = Artist.id)
        WHERE Artist.name = ?";
    let (sender, receiver) = channel::<String>();
    let _sub = db2.query_subscribe(sql, ("Metallica",), move |albums: Vec<Album>| {
        println!("Albums by Metallica: {:?}", albums);
        for album in albums {
            sender.send(album.title).unwrap();
        }
    })?;

    let sync = SyncEngine::builder()
        .in_memory()
        .build()?;
    sync.sync(&db1)?;
    sync.sync(&db2)?;

    assert_eq!(receiver.recv()?, "...And Justice For All");

    Ok(())
}

#[test]
fn test_main() -> Result<()> {
    main()
}

