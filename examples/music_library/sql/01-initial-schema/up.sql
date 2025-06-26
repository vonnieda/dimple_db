CREATE TABLE Artist (
    key TEXT NOT NULL PRIMARY KEY, 
    name TEXT NOT NULL,
    disambiguation TEXT
);

CREATE TABLE Album (
    key TEXT NOT NULL PRIMARY KEY, 
    title TEXT NOT NULL,
    disambiguation TEXT
);

CREATE TABLE Genre (
    key TEXT NOT NULL PRIMARY KEY, 
    name TEXT NOT NULL
);

CREATE TABLE ArtistGenre (
    key TEXT NOT NULL PRIMARY KEY,
    artist_key NOT NULL,
    genre_key NOT NULL,
    FOREIGN KEY (artist_key) REFERENCES Artist(key),
    FOREIGN KEY (genre_key) REFERENCES Genre(key)
);

CREATE TABLE AlbumArtist (
    key TEXT NOT NULL PRIMARY KEY,
    album_key NOT NULL,
    artist_key NOT NULL,
    FOREIGN KEY (album_key) REFERENCES Album(key),
    FOREIGN KEY (artist_key) REFERENCES Artist(key)
);

CREATE TABLE AlbumGenre (
    key TEXT NOT NULL PRIMARY KEY,
    album_key NOT NULL,
    genre_key NOT NULL,
    FOREIGN KEY (album_key) REFERENCES Album(key),
    FOREIGN KEY (genre_key) REFERENCES Genre(key)
);
