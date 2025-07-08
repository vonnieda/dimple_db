use dimple_db::{sync::{InMemoryStorage, Sync}, Db};
use rusqlite_migration::{Migrations, M};
use serde::{Deserialize, Serialize};

fn main() -> anyhow::Result<()> {
    let db1 = Db::open_memory()?;
    let db2 = Db::open_memory()?;
    let sync = Sync::new(Box::new(InMemoryStorage::new()))?;

    let migrations = Migrations::new(vec![
        M::up("
            CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL);
            CREATE TABLE Album (id TEXT PRIMARY KEY, title TEXT NOT NULL);
            CREATE TABLE AlbumArtist (
                id TEXT PRIMARY KEY, album_id TEXT NOT NULL, artist_id TEXT NOT NULL, 
                FOREIGN KEY (album_id) REFERENCES Album(id),
                FOREIGN KEY (artist_id) REFERENCES Artist(id)
            );
        "),
        M::up("ALTER TABLE Artist ADD COLUMN summary TEXT;"),
    ]);
    db1.migrate(&migrations)?;
    db2.migrate(&migrations)?;

    let sql = "SELECT Album.* FROM Album 
        JOIN Album_Artist ON (Album_Artist.album_id = Album.id)
        JOIN Artist ON (Album_Artist.artist_id = Artist.id)
        WHERE Artist.name = ?";
    // db2.query(sql, ("Metallica",)).subscribe(|albums: Vec<Album>| {
    //     println!("")
    // })?;

    

    // let artist = db.save(&Artist {
    //     name: "Metallica".to_string(),
    //     ..Default::default()
    // })?;
    // let sql = "SELECT Album.* FROM Album 
    //     JOIN Album_Artist ON (Album_Artist.album_id = Album.id)
    //     WHERE Album_Artist.artist_id = ?";

    // // The core is really two things: tracking which tables a query depends on,
    // // and notifing when one of them changes.
    // db.query_subscribe(sql, (&artist.id,), |albums: Vec<Album>| {
    //     dbg!(albums);
    // })?;
    // let album = db.save(&Album {
    //     title: "...And Justice For All".to_string(),
    //     ..Default::default()
    // })?;
    // db.save(&AlbumArtist {
    //     album_id: album.id,
    //     artist_id: artist.id.clone(),
    //     ..Default::default()
    // })?;
    // std::thread::sleep(Duration::from_millis(100));
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

impl Album {

}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct AlbumArtist {
    pub id: String,
    pub album_id: String,
    pub artist_id: String,
}


