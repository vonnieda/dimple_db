use std::collections::HashSet;

use super::core::Db;
use super::types::Entity;

impl Db {
    // TODO change this to return an object that will either allow getting
    // the results as a vec or subscribing to get them as with observe_query,
    // like this:
    // let artists: Vec<Artist> = db.query("SELECT * FROM Artist").to_vec();
    // let query = db.query("SELECT * FROM Artist").subscribe(|artists: Vec<Artist>| dbg!(artists));
    // // callback is called whenever Artists change
    // drop(query); to cancel the subscription
    pub fn query<T: Entity>(
        &self,
        sql: &str,
        params: &[&dyn rusqlite::ToSql],
    ) -> anyhow::Result<Vec<T>> {
        log::debug!("SQL QUERY: {}", sql);

        let conn = self
            .conn
            .read()
            .map_err(|_| anyhow::anyhow!("Failed to acquire read lock"))?;

        let mut stmt = conn.prepare(sql)?;
        let mut rows = stmt.query(params)?;

        let mut results = Vec::new();
        while let Some(row) = rows.next()? {
            let entity = serde_rusqlite::from_row::<T>(row)?;
            results.push(entity);
        }

        log::debug!("SQL QUERY RESULT: {} rows", results.len());
        Ok(results)
    }

    pub(crate) fn extract_table_dependencies(
        &self,
        sql: &str,
        params: &[&dyn rusqlite::ToSql],
    ) -> anyhow::Result<HashSet<String>> {
        let conn = self
            .conn
            .read()
            .map_err(|_| anyhow::anyhow!("Failed to acquire read lock"))?;

        let explain_sql = format!("EXPLAIN QUERY PLAN {}", sql);
        let mut stmt = conn.prepare(&explain_sql)?;
        let mut rows = stmt.query(params)?;

        let mut tables = HashSet::new();

        while let Some(row) = rows.next()? {
            // EXPLAIN QUERY PLAN columns: id, parent, notused, detail
            let detail: String = row.get(3)?;

            // Parse table names from the detail column
            // Examples: "SCAN TABLE Artist", "SEARCH TABLE Artist USING INDEX"
            if let Some(table_name) = self.extract_table_from_detail(&detail) {
                tables.insert(table_name);
            }
        }
        Ok(tables)
    }

    fn extract_table_from_detail(&self, detail: &str) -> Option<String> {
        // Handle common patterns in EXPLAIN QUERY PLAN output
        let detail_upper = detail.to_uppercase();

        if detail_upper.starts_with("SCAN TABLE ") {
            // "SCAN TABLE Artist" -> "Artist"
            if let Some(table_name) = detail_upper.strip_prefix("SCAN TABLE ") {
                return Some(table_name.split_whitespace().next()?.to_string());
            }
        } else if detail_upper.starts_with("SCAN ") {
            // "SCAN Artist" -> "Artist" (simpler SQLite format)
            if let Some(table_name) = detail_upper.strip_prefix("SCAN ") {
                return Some(table_name.split_whitespace().next()?.to_string());
            }
        } else if detail_upper.starts_with("SEARCH TABLE ") {
            // "SEARCH TABLE Artist USING INDEX idx_name" -> "Artist"
            if let Some(rest) = detail_upper.strip_prefix("SEARCH TABLE ") {
                return Some(rest.split_whitespace().next()?.to_string());
            }
        } else if detail_upper.starts_with("SEARCH ") {
            // "SEARCH Artist USING INDEX idx_name" -> "Artist" (simpler format)
            if let Some(rest) = detail_upper.strip_prefix("SEARCH ") {
                return Some(rest.split_whitespace().next()?.to_string());
            }
        } else if detail_upper.contains(" TABLE ") {
            // Generic fallback for other patterns
            if let Some(start) = detail_upper.find(" TABLE ") {
                let after_table = &detail_upper[start + 7..]; // 7 = len(" TABLE ")
                return Some(after_table.split_whitespace().next()?.to_string());
            }
        }

        None
    }

    pub(crate) fn calculate_result_hash_from_json(&self, data: &[serde_json::Value]) -> anyhow::Result<u64> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        let json_str = serde_json::to_string(data)?;
        json_str.hash(&mut hasher);
        Ok(hasher.finish())
    }

    pub(crate) fn calculate_result_hash<T: Entity>(&self, data: &[T]) -> anyhow::Result<u64> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();

        // Hash the serialized data for comparison
        for item in data {
            let json_str = serde_json::to_string(item)?;
            json_str.hash(&mut hasher);
        }

        Ok(hasher.finish())
    }

    pub(crate) fn extract_keys_from_json_results(
        &self,
        data: &[serde_json::Value],
    ) -> anyhow::Result<HashSet<String>> {
        let mut keys = HashSet::new();

        for item in data {
            if let Some(obj) = item.as_object() {
                if let Some(key_value) = obj.get("key") {
                    if let Some(key_str) = key_value.as_str() {
                        keys.insert(key_str.to_string());
                    }
                }
            }
        }

        Ok(keys)
    }

    pub(crate) fn extract_keys_from_results<T: Entity>(&self, data: &[T]) -> anyhow::Result<HashSet<String>> {
        let mut keys = HashSet::new();

        for item in data {
            let entity_json = serde_json::to_value(item)?;
            let key = self.extract_key_value_from_json(&entity_json)?;
            keys.insert(key);
        }

        Ok(keys)
    }

    pub(crate) fn query_as_json_with_params(
        &self,
        sql: &str,
        params: &[rusqlite::types::Value],
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        let conn = self
            .conn
            .read()
            .map_err(|_| anyhow::anyhow!("Failed to acquire read lock"))?;
        let mut stmt = conn.prepare(sql)?;
        let mut rows = stmt.query(rusqlite::params_from_iter(params))?;

        let mut results = Vec::new();
        while let Some(row) = rows.next()? {
            let column_count = row.as_ref().column_count();
            let mut json_map = serde_json::Map::new();

            for i in 0..column_count {
                let column_name = row.as_ref().column_name(i)?;
                let value: rusqlite::types::Value = row.get(i)?;

                let json_value = match value {
                    rusqlite::types::Value::Null => serde_json::Value::Null,
                    rusqlite::types::Value::Integer(i) => serde_json::Value::Number(i.into()),
                    rusqlite::types::Value::Real(f) => serde_json::Value::Number(
                        serde_json::Number::from_f64(f).unwrap_or(serde_json::Number::from(0)),
                    ),
                    rusqlite::types::Value::Text(s) => serde_json::Value::String(s),
                    rusqlite::types::Value::Blob(_) => {
                        serde_json::Value::String("<binary>".to_string())
                    }
                };

                json_map.insert(column_name.to_string(), json_value);
            }

            results.push(serde_json::Value::Object(json_map));
        }

        Ok(results)
    }

    pub(crate) fn serialize_tosql_param(&self, param: &dyn rusqlite::ToSql) -> anyhow::Result<String> {
        // Convert ToSql parameter to a serializable string representation
        // We use SQLite's value conversion to get a consistent representation
        let conn = self
            .conn
            .read()
            .map_err(|_| anyhow::anyhow!("Failed to acquire read lock"))?;
        let mut stmt = conn.prepare("SELECT ?")?;
        let mut rows = stmt.query([param])?;

        if let Some(row) = rows.next()? {
            let value: rusqlite::types::Value = row.get(0)?;
            Ok(match value {
                rusqlite::types::Value::Null => "NULL".to_string(),
                rusqlite::types::Value::Integer(i) => format!("INT:{}", i),
                rusqlite::types::Value::Real(f) => format!("REAL:{}", f),
                rusqlite::types::Value::Text(s) => format!("TEXT:{}", s),
                rusqlite::types::Value::Blob(_) => {
                    return Err(anyhow::anyhow!("Blob parameters not supported"));
                }
            })
        } else {
            Err(anyhow::anyhow!("Failed to serialize parameter"))
        }
    }

    pub(crate) fn deserialize_params(
        &self,
        serialized_params: &[String],
    ) -> anyhow::Result<Vec<rusqlite::types::Value>> {
        serialized_params
            .iter()
            .map(|s| {
                if s == "NULL" {
                    Ok(rusqlite::types::Value::Null)
                } else if let Some(int_str) = s.strip_prefix("INT:") {
                    Ok(rusqlite::types::Value::Integer(int_str.parse()?))
                } else if let Some(real_str) = s.strip_prefix("REAL:") {
                    Ok(rusqlite::types::Value::Real(real_str.parse()?))
                } else if let Some(text_str) = s.strip_prefix("TEXT:") {
                    Ok(rusqlite::types::Value::Text(text_str.to_string()))
                } else {
                    Err(anyhow::anyhow!("Invalid parameter format: {}", s))
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Clone, Default, Debug)]
    pub struct Artist {
        pub key: String,
        pub name: String,
        pub disambiguation: Option<String>,
    }

    #[test]
    fn test_query() -> anyhow::Result<()> {
        let db = Db::open_memory()?;
        if let Ok(conn) = db.conn.write() {
            conn.execute_batch(
                "
                CREATE TABLE Artist (
                    key            TEXT NOT NULL PRIMARY KEY,
                    name           TEXT NOT NULL,
                    disambiguation TEXT
                );
            ",
            )?;
        }

        // Insert some test data
        let artist1 = db.save(&Artist {
            name: "Metallica".to_string(),
            disambiguation: Some("American metal band".to_string()),
            ..Default::default()
        })?;

        let _artist2 = db.save(&Artist {
            name: "Iron Maiden".to_string(),
            disambiguation: Some("British metal band".to_string()),
            ..Default::default()
        })?;

        // Test query all
        let all_artists: Vec<Artist> = db.query("SELECT * FROM Artist ORDER BY name", &[])?;
        assert_eq!(all_artists.len(), 2);
        assert_eq!(all_artists[0].name, "Iron Maiden");
        assert_eq!(all_artists[1].name, "Metallica");

        // Test query with parameters
        let filtered_artists: Vec<Artist> =
            db.query("SELECT * FROM Artist WHERE name = ?", &[&"Metallica"])?;
        assert_eq!(filtered_artists.len(), 1);
        assert_eq!(filtered_artists[0].name, "Metallica");
        assert_eq!(filtered_artists[0].key, artist1.key);

        // Test query with no results
        let no_artists: Vec<Artist> =
            db.query("SELECT * FROM Artist WHERE name = ?", &[&"NonExistent"])?;
        assert_eq!(no_artists.len(), 0);

        Ok(())
    }

    #[test]
    fn test_explain_query_plan_parsing() -> anyhow::Result<()> {
        let db = crate::Db::open_memory()?;

        // Create test table
        if let Ok(conn) = db.conn.write() {
            conn.execute_batch(
                "
                CREATE TABLE Artist (
                    key            TEXT NOT NULL PRIMARY KEY,
                    name           TEXT NOT NULL,
                    disambiguation TEXT
                );
            ",
            )?;
        }

        // Test table dependency extraction
        let tables = db.extract_table_dependencies("SELECT * FROM Artist", &[])?;
        assert!(tables.contains("ARTIST")); // SQLite normalizes to uppercase

        Ok(())
    }
}