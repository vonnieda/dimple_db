use dimple_data::Db;
use include_dir::{include_dir, Dir};
use rusqlite_migration::Migrations;

use crate::types::{Album, AlbumArtist, Artist};

mod types;

fn main() -> anyhow::Result<()> {
    let db = Db::open_memory()?;
    static MIGRATION_DIR: Dir = include_dir!("./examples/music_library/sql");
    let migrations = Migrations::from_directory(&MIGRATION_DIR).unwrap();
    db.migrate(&migrations)?;
    let artist = db.save(&Artist {
        name: "Metallica".to_string(),
        ..Default::default()
    })?;
    let album = db.save(&Album {
        title: "Master of Puppets".to_string(),
        ..Default::default()
    })?;
    db.save(&AlbumArtist {
        album_key: album.key,
        artist_key: artist.key.clone(),
        ..Default::default()
    })?;
    let album = db.save(&Album {
        title: "...And Justice For All".to_string(),
        ..Default::default()
    })?;
    db.save(&AlbumArtist {
        album_key: album.key,
        artist_key: artist.key.clone(),
        ..Default::default()
    })?;
    let artist = db.save(&Artist {
        name: "Megadeth".to_string(),
        ..Default::default()
    })?;
    let album = db.save(&Album {
        title: "Rust in Peace".to_string(),
        ..Default::default()
    })?;
    db.save(&AlbumArtist {
        album_key: album.key,
        artist_key: artist.key.clone(),
        ..Default::default()
    })?;

    let artists: Vec<Artist> = db.query("SELECT * FROM Artist ORDER BY name ASC", &[])?;
    for artist in artists {
        let albums: Vec<Album> = db.query("SELECT Album.* 
            FROM Album 
            JOIN AlbumArtist ON (AlbumArtist.album_key = Album.key)
            WHERE AlbumArtist.artist_key = ?", &[&artist.key])?;
        for album in albums {
            println!("{:20} {:20}", &artist.name, album.title);
        }
    }

    Ok(())
}