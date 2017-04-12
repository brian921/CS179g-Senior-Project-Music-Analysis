CREATE KEYSPACE musicnet_metadata WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':3};

CREATE TABLE musicnet_metadata.metadata
(
        ID int PRIMARY KEY, 
        composer varchar,
        composition varchar,
        movement varchar,
        ensemble varchar,
        source varchar,
        transcriber varchar,
        catalog_name varchar,
        seconds int
);

CREATE TABLE musicnet_metadata.song
(
        start_time varchar,
        end_time varchar,
        instrument_id varchar,
        note_id varchar,
        measure varchar,
        beat varchar,
        note_value varchar,
        song_id varchar PRIMARY KEY
);

CREATE TABLE musicnet_metadata.songkey
(
	ID int PRIMARY KEY, 
	song_key varchar,
);

CREATE TABLE musicnet_metadata.chord
(
	ID int, 
	chord_value varchar,
	measure varchar,
	chord_id varchar PRIMARY KEY
);
