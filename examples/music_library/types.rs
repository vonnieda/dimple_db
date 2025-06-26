use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct Artist {
    pub key: String,
    pub name: String,
    pub disambiguation: Option<String>,
    pub summary: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct Album {
    pub key: String,
    pub title: String,
    pub disambiguation: Option<String>,
    pub summary: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct Genre {
    pub key: String,
    pub name: String,
    pub summary: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct ArtistGenre {
    pub key: String,
    pub artist_key: String,
    pub genre_key: String,
}

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct AlbumArtist {
    pub key: String,
    pub album_key: String,
    pub artist_key: String,
}

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct AlbumGenre {
    pub key: String,
    pub album_key: String,
    pub genre_key: String,
}