# Sync Engine

1. All changes to entities are tracked in the ZV_CHANGE and associated tables:
```
CREATE TABLE IF NOT EXISTS ZV_METADATA (
	key TEXT NOT NULL PRIMARY KEY,
	value TEXT NOT NULL
);

INSERT OR IGNORE INTO ZV_METADATA (key, value) 
	VALUES ('database_uuid', uuid7());

CREATE TABLE IF NOT EXISTS ZV_TRANSACTION (
	id TEXT NOT NULL PRIMARY KEY,
	author TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ZV_CHANGE (
	id TEXT NOT NULL PRIMARY KEY,
	transaction_id TEXT NOT NULL,
	entity_type TEXT NOT NULL,
	entity_id TEXT NOT NULL,
	attribute TEXT NOT NULL,
	old_value TEXT,
	new_value TEXT,
	FOREIGN KEY (transaction_id) REFERENCES ZV_TRANSACTION(id)
);
```
2. Each replica keeps a complete merged copy of the change log ZV_CHANGES.
3. Storage Directory Structure
	TODO this structure has a bug - we have to list all paths in the base path to get the 
	list of devices.

So maybe it's more like:
- s3://endpoint/bucket/base_path
	- replicas/{replica_uuid}.json
	- changes/{replica_uuid}/{change_uuid}.json

So then:
replicas = list("replicas/")
replica_changes = list("changes/{replica_uuid}/")
all_changes = list("changes/")




	- s3://endpoint/bucket/base_path
		- {device_1_database_uuid}.dimple_db
			- ZV_CHANGE/
				- LATEST.json -> Contains info about latest file uploaded, including the first and last change_ids in the file and in the future probably a hash chain. 
				- {change_uuid_1}.db
				- {change_uuid_N}.db
		- {device_N_database_uuid}.dimple_db
4. Sync Algorithm
	1. Pull new remote changes
		1. Get list of replicas by listing `*.dimple_db` and extracting the UUID part.
		2. (Optionally) Register unknown replicas, "Want to sync with replica id XYZ?", and then save their public key.
		3. For each replica:
			1. Read LATEST.json to get the id of the newest change.
			2. See what the latest change stored for this replica is: `SELECT * FROM ZV_CHANGE WHERE author_id = {} ORDER BY id DESC LIMIT 1`
			3. If there are newer changes, download the new files in order
			4. For each new change
				1. Insert the change into the ZV_CHANGE table
				2. If the change is newer than the last change to the given (entity, key, attribute) then update the entity in the database as well
	2. Push new local changes
		1. Read LATEST.json to get the id of the latest change uploaded
		2. If there are any newer changes stored:
			1. Upload a new Sqlite database containing just the new changes in ZV_CHANGE
			2. Update LATEST.json with the first and last change ids from the file just uploaded.

