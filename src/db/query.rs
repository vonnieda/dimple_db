use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{channel, Sender};
use std::thread::{self, JoinHandle};
use anyhow::Result;
use rusqlite::Params;
use rusqlite::{types::{Value, ToSql}};
use crate::db::{Db, Entity, DbEvent};

/// Handle returned to the user for managing a query subscription
pub struct QuerySubscription<P: Params + Clone + Send + 'static> {
    db: Db,
    sql: String,
    params: P,
    dependent_tables: HashSet<String>,
    stop_signal: Option<Sender<()>>,
    thread_handle: Option<JoinHandle<()>>,
}

impl<P: Params + Clone + Send + 'static> QuerySubscription<P> {
    pub fn new<E: Entity + 'static, F>(db: &Db, sql: &str, params: P, callback: F) -> Result<Self> 
    where 
        F: FnMut(Vec<E>) + Send + 'static
    {        
        let dependent_tables = QuerySubscription::<()>::extract_query_tables(sql, ())?;
        
        // Wrap the callback in Arc<Mutex<>> for thread safety
        let callback = Arc::new(Mutex::new(callback));
        
        // Run the query initially to provide immediate results
        let initial_results: Vec<E> = db.query(sql, params.clone())?;
        if let Ok(mut cb) = callback.lock() {
            cb(initial_results);
        }
        
        // Create stop signal channel
        let (stop_tx, stop_rx) = channel::<()>();
        
        // Clone values needed for the thread
        let db_clone = db.clone();
        let sql_clone = sql.to_string();
        let params_clone = params.clone();
        let tables_clone = dependent_tables.clone();
        let callback_clone = callback.clone();
        
        // Create the monitoring thread
        let thread_handle = thread::spawn(move || {
            let event_rx = db_clone.subscribe();
            
            loop {
                // Check for stop signal
                if stop_rx.try_recv().is_ok() {
                    break;
                }
                
                // Check for database events (with timeout to allow periodic stop checks)
                match event_rx.recv_timeout(std::time::Duration::from_millis(100)) {
                    Ok(event) => {
                        // Check if this event affects our query
                        let table_name = match &event {
                            DbEvent::Insert(table, _) => table,
                            DbEvent::Update(table, _) => table,
                            DbEvent::Delete(table, _) => table,
                        };
                        
                        if tables_clone.contains(table_name) {
                            // Add a small delay to avoid database lock issues
                            // TODO no!
                            thread::sleep(std::time::Duration::from_millis(10));
                            
                            // Re-run the query
                            match db_clone.query::<E, _>(sql_clone.as_str(), params_clone.clone()) {
                                Ok(results) => {
                                    if let Ok(mut cb) = callback_clone.lock() {
                                        cb(results);
                                    }
                                },
                                Err(e) => eprintln!("Error re-running query: {}", e),
                            }
                        }
                    },
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        // Timeout is fine, just check stop signal again
                        continue;
                    },
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                        // Database subscription ended
                        break;
                    }
                }
            }
        });
        
        Ok(QuerySubscription {
            db: db.clone(),
            sql: sql.to_string(),
            params,
            dependent_tables,
            stop_signal: Some(stop_tx),
            thread_handle: Some(thread_handle),
        })
    }

    pub fn unsubscribe(&mut self) {
        // Send stop signal to the thread
        if let Some(stop_signal) = self.stop_signal.take() {
            let _ = stop_signal.send(()); // Ignore error if receiver already dropped
        }
        
        // Wait for the thread to finish
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join(); // Ignore error if thread panicked
        }
    }
}

// Static methods that don't depend on the generic parameter P
impl QuerySubscription<()> {
    /// Converts common parameter types to a storable format (Vec<Value>) for later reuse
    pub fn serialize_params<Q: IntoIterator<Item = T>, T: ToSql>(params: Q) -> Result<Vec<Value>> {
        let mut values = Vec::new();
        
        for param in params {
            let sql_output = param.to_sql()?;
            let stored_value = match sql_output {
                rusqlite::types::ToSqlOutput::Borrowed(value_ref) => match value_ref {
                    rusqlite::types::ValueRef::Null => Value::Null,
                    rusqlite::types::ValueRef::Integer(i) => Value::Integer(i),
                    rusqlite::types::ValueRef::Real(r) => Value::Real(r),
                    rusqlite::types::ValueRef::Text(t) => {
                        Value::Text(String::from_utf8_lossy(t).to_string())
                    }
                    rusqlite::types::ValueRef::Blob(b) => Value::Blob(b.to_vec()),
                },
                rusqlite::types::ToSqlOutput::Owned(value) => value,
                _ => return Err(anyhow::anyhow!("Unsupported ToSqlOutput variant")),
            };
            values.push(stored_value);
        }
        
        Ok(values)
    }

    /// Converts stored Values back to a format that can be used as query parameters
    pub fn deserialize_params(values: &[Value]) -> Vec<Value> {
        // Since Value already implements ToSql, we can just clone the values
        // The caller will need to convert this to a slice or use params_from_iter
        values.to_vec()
    }

    /// Extracts table names from a SQL query.
    /// Uses a simple regex-based approach to find table names in FROM and JOIN clauses.
    /// TODO remove params, not needed
    pub fn extract_query_tables(sql: &str, _params: impl rusqlite::Params) -> Result<HashSet<String>> {
        let mut tables = HashSet::new();
        
        // Normalize the SQL to uppercase for easier parsing
        let sql_upper = sql.to_uppercase();
        
        // Find FROM clause
        if let Some(from_start) = sql_upper.find("FROM ") {
            let from_sql = &sql[from_start + 5..];
            
            // Find the end of the FROM clause (before WHERE, JOIN, GROUP BY, etc.)
            let end_keywords = ["WHERE ", "JOIN ", "LEFT JOIN ", "RIGHT JOIN ", "FULL JOIN ", "CROSS JOIN ", "INNER JOIN ", "GROUP BY ", "ORDER BY ", "HAVING ", "LIMIT "];
            let mut end_pos = from_sql.len();
            for keyword in &end_keywords {
                if let Some(pos) = from_sql.to_uppercase().find(keyword) {
                    end_pos = end_pos.min(pos);
                }
            }
            
            let from_clause = &from_sql[..end_pos];
            
            // Split by comma to handle multiple tables
            for table_part in from_clause.split(',') {
                if let Some(table_name) = Self::extract_table_name(table_part.trim()) {
                    tables.insert(table_name);
                }
            }
        }
        
        // Find all JOIN clauses
        let join_keywords = ["JOIN ", "INNER JOIN ", "LEFT JOIN ", "RIGHT JOIN ", "FULL JOIN ", "CROSS JOIN "];
        for keyword in &join_keywords {
            let mut search_pos = 0;
            while let Some(join_pos) = sql_upper[search_pos..].find(keyword) {
                let actual_pos = search_pos + join_pos + keyword.len();
                let join_sql = &sql[actual_pos..];
                
                if let Some(table_name) = Self::extract_table_name(join_sql) {
                    tables.insert(table_name);
                }
                
                search_pos = actual_pos;
            }
        }
        
        Ok(tables)
    }
    
    /// Extract a table name from the beginning of a SQL fragment
    /// Handles "TableName", "TableName alias", "TableName AS alias"
    pub fn extract_table_name(sql_fragment: &str) -> Option<String> {
        let trimmed = sql_fragment.trim();
        
        // Split by whitespace and take the first token
        let first_token = trimmed.split_whitespace().next()?;
        
        // Remove any trailing punctuation like commas
        let clean_name = first_token.trim_matches(|c: char| !c.is_alphanumeric() && c != '_');
        
        // Validate it looks like a table name
        if !clean_name.is_empty() && clean_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            Some(clean_name.to_string())
        } else {
            None
        }
    }
}

impl<P: Params + Clone + Send + 'static> Drop for QuerySubscription<P> {
    fn drop(&mut self) {
        self.unsubscribe();
    }   
}

impl Db {
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite_migration::{Migrations, M};
    use rusqlite::types::ToSql;
    use serde::{Deserialize, Serialize};

    fn setup_db() -> Result<Db> {
        let db = Db::open_memory()?;
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL, summary TEXT);"),
        ]);
        db.migrate(&migrations)?;
        Ok(db)
    }

    #[test]
    fn extract_query_tables_simple_select() -> Result<()> {
        let db = setup_db()?;
        let tables = QuerySubscription::extract_query_tables("SELECT * FROM Artist WHERE id = ?", ["test_id"])?;
        assert_eq!(tables.len(), 1);
        assert!(tables.contains("Artist"));
        Ok(())
    }

    #[test]
    fn extract_query_tables_with_join() -> Result<()> {
        let db = Db::open_memory()?;
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL);"),
            M::up("CREATE TABLE Album (id TEXT PRIMARY KEY, title TEXT NOT NULL, artist_id TEXT);"),
        ]);
        db.migrate(&migrations)?;
        
        let tables = QuerySubscription::extract_query_tables(
            "SELECT a.name, al.title FROM Artist a JOIN Album al ON a.id = al.artist_id WHERE a.id = ?",
            ["test_id"]
        )?;
        assert_eq!(tables.len(), 2);
        assert!(tables.contains("Artist"));
        assert!(tables.contains("Album"));
        Ok(())
    }

    #[test]
    fn extract_query_tables_multiple_joins() -> Result<()> {
        let db = Db::open_memory()?;
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL);"),
            M::up("CREATE TABLE Album (id TEXT PRIMARY KEY, title TEXT NOT NULL, artist_id TEXT);"),
            M::up("CREATE TABLE Track (id TEXT PRIMARY KEY, title TEXT NOT NULL, album_id TEXT);"),
        ]);
        db.migrate(&migrations)?;
        
        let tables = QuerySubscription::extract_query_tables(
            "SELECT a.name, al.title, t.title 
             FROM Artist a 
             LEFT JOIN Album al ON a.id = al.artist_id 
             INNER JOIN Track t ON al.id = t.album_id 
             WHERE a.name = ?",
            ["Beatles"]
        )?;
        assert_eq!(tables.len(), 3);
        assert!(tables.contains("Artist"));
        assert!(tables.contains("Album"));
        assert!(tables.contains("Track"));
        Ok(())
    }
    
    #[test]
    fn extract_query_tables_subquery() -> Result<()> {
        let db = setup_db()?;
        
        // Note: Our simple parser won't handle subqueries perfectly,
        // but it should at least find the main table
        let tables = QuerySubscription::extract_query_tables(
            "SELECT * FROM Artist WHERE id IN (SELECT artist_id FROM Album WHERE title = ?)",
            ["Abbey Road"]
        )?;
        assert!(tables.contains("Artist"));
        // Our simple parser might miss the subquery table
        Ok(())
    }

    // Unhappy path tests
    #[test]
    fn extract_query_tables_no_from_clause() -> Result<()> {
        let db = setup_db()?;
        
        // Query without FROM clause
        let tables = QuerySubscription::extract_query_tables("SELECT 1 + 1", [])?;
        assert_eq!(tables.len(), 0);
        Ok(())
    }

    #[test]
    fn extract_query_tables_malformed_sql() -> Result<()> {
        let db = setup_db()?;
        
        // Malformed SQL should not panic, just return empty or partial results
        let tables = QuerySubscription::extract_query_tables("SELECT * FORM Artist", [])?;
        assert_eq!(tables.len(), 0); // FORM instead of FROM
        
        let tables = QuerySubscription::extract_query_tables("FROM Artist SELECT *", [])?;
        assert!(tables.contains("Artist")); // Should still find the table
        Ok(())
    }

    #[test]
    fn extract_query_tables_special_characters() -> Result<()> {
        let db = setup_db()?;
        
        // Table names with special characters (though not recommended)
        let tables = QuerySubscription::extract_query_tables("SELECT * FROM `Artist-Table`", [])?;
        assert_eq!(tables.len(), 0); // Our parser expects alphanumeric names
        
        // Table with numbers and underscores (valid)
        let tables = QuerySubscription::extract_query_tables("SELECT * FROM Artist_2024", [])?;
        assert!(tables.contains("Artist_2024"));
        Ok(())
    }

    #[test]
    fn extract_query_tables_case_sensitivity() -> Result<()> {
        let db = setup_db()?;
        
        // Mixed case queries
        let tables = QuerySubscription::extract_query_tables("select * from Artist", [])?;
        assert!(tables.contains("Artist"));
        
        let tables = QuerySubscription::extract_query_tables("SELECT * FROM artist", [])?;
        assert!(tables.contains("artist"));
        
        let tables = QuerySubscription::extract_query_tables("SeLeCt * FrOm ArTiSt", [])?;
        assert!(tables.contains("ArTiSt"));
        Ok(())
    }

    #[test]
    fn extract_query_tables_empty_and_whitespace() -> Result<()> {
        let db = setup_db()?;
        
        // Empty query
        let tables = QuerySubscription::extract_query_tables("", [])?;
        assert_eq!(tables.len(), 0);
        
        // Only whitespace
        let tables = QuerySubscription::extract_query_tables("   \t\n   ", [])?;
        assert_eq!(tables.len(), 0);
        
        // Extra whitespace around tables
        let tables = QuerySubscription::extract_query_tables("SELECT * FROM    Artist    ", [])?;
        assert!(tables.contains("Artist"));
        Ok(())
    }

    #[test]
    fn extract_query_tables_comments() -> Result<()> {
        let db = setup_db()?;
        
        // SQL comments (our simple parser doesn't handle these)
        let tables = QuerySubscription::extract_query_tables(
            "SELECT * FROM Artist -- this is a comment",
            []
        )?;
        assert!(tables.contains("Artist"));
        
        // Comment that looks like a table
        // NOTE: Our simple parser doesn't strip comments, so it will find "Album" in the comment
        let tables = QuerySubscription::extract_query_tables(
            "SELECT * FROM Artist /* JOIN Album */",
            []
        )?;
        assert_eq!(tables.len(), 2); // Finds both Artist and Album
        assert!(tables.contains("Artist"));
        assert!(tables.contains("Album")); // Found in the comment
        Ok(())
    }

    #[test]
    fn extract_query_tables_with_parentheses() -> Result<()> {
        let db = setup_db()?;
        
        // Tables in parentheses (common in complex queries)
        // NOTE: Our parser actually strips parentheses as punctuation, so it finds the table
        let tables = QuerySubscription::extract_query_tables(
            "SELECT * FROM (Artist) WHERE id = ?",
            ["test"]
        )?;
        assert_eq!(tables.len(), 1); // Parser strips parentheses and finds Artist
        assert!(tables.contains("Artist"));
        
        // Comma-separated tables should now work
        let tables = QuerySubscription::extract_query_tables(
            "SELECT * FROM Artist, Album WHERE Artist.id = Album.artist_id",
            []
        )?;
        assert_eq!(tables.len(), 2);
        assert!(tables.contains("Artist"));
        assert!(tables.contains("Album"));
        Ok(())
    }

    #[test]
    fn extract_query_tables_reserved_keywords() -> Result<()> {
        let db = setup_db()?;
        
        // Using a keyword that contains JOIN
        let tables = QuerySubscription::extract_query_tables(
            "SELECT * FROM JOINED_TABLE",
            []
        )?;
        assert!(tables.contains("JOINED_TABLE"));
        
        // Table name that starts with a keyword
        let tables = QuerySubscription::extract_query_tables(
            "SELECT * FROM FROM_TABLE",
            []
        )?;
        assert!(tables.contains("FROM_TABLE"));
        Ok(())
    }

    #[test]
    fn extract_query_tables_comma_separated() -> Result<()> {
        let db = setup_db()?;
        
        // Comma-separated tables (old-style JOIN)
        let tables = QuerySubscription::extract_query_tables(
            "SELECT * FROM Artist, Album WHERE Artist.id = Album.artist_id",
            []
        )?;
        assert_eq!(tables.len(), 2);
        assert!(tables.contains("Artist"));
        assert!(tables.contains("Album"));
        
        // Comma-separated tables with aliases
        let tables = QuerySubscription::extract_query_tables(
            "SELECT * FROM Artist a, Album al WHERE a.id = al.artist_id",
            []
        )?;
        assert_eq!(tables.len(), 2);
        assert!(tables.contains("Artist"));
        assert!(tables.contains("Album"));
        
        // Multiple comma-separated tables
        let tables = QuerySubscription::extract_query_tables(
            "SELECT * FROM Artist, Album, Track WHERE Artist.id = Album.artist_id",
            []
        )?;
        assert_eq!(tables.len(), 3);
        assert!(tables.contains("Artist"));
        assert!(tables.contains("Album"));
        assert!(tables.contains("Track"));
        Ok(())
    }

    #[test]
    fn extract_query_tables_mixed_comma_and_join() -> Result<()> {
        let db = Db::open_memory()?;
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL);"),
            M::up("CREATE TABLE Album (id TEXT PRIMARY KEY, title TEXT NOT NULL, artist_id TEXT);"),
            M::up("CREATE TABLE Track (id TEXT PRIMARY KEY, title TEXT NOT NULL, album_id TEXT);"),
            M::up("CREATE TABLE Genre (id TEXT PRIMARY KEY, name TEXT NOT NULL);"),
        ]);
        db.migrate(&migrations)?;
        
        // Comma-separated tables with additional JOINs
        let tables = QuerySubscription::extract_query_tables(
            "SELECT * FROM Artist, Album JOIN Track ON Album.id = Track.album_id WHERE Artist.id = Album.artist_id",
            []
        )?;
        assert_eq!(tables.len(), 3);
        assert!(tables.contains("Artist"));
        assert!(tables.contains("Album"));
        assert!(tables.contains("Track"));
        Ok(())
    }

    // Parameter serialization tests
    #[test]
    fn serialize_params_empty() -> Result<()> {
        let db = setup_db()?;
        let empty_vec: Vec<&str> = vec![];
        let values = QuerySubscription::serialize_params(empty_vec)?;
        assert_eq!(values.len(), 0);
        Ok(())
    }

    #[test]
    fn serialize_params_various_types() -> Result<()> {
        let db = setup_db()?;
        
        // Test different parameter types
        let params = vec!["text_param", "123", "45.67"];
        let values = QuerySubscription::serialize_params(params)?;
        
        assert_eq!(values.len(), 3);
        match &values[0] {
            Value::Text(s) => assert_eq!(s, "text_param"),
            _ => panic!("Expected text value"),
        }
        match &values[1] {
            Value::Text(s) => assert_eq!(s, "123"),
            _ => panic!("Expected text value"),
        }
        match &values[2] {
            Value::Text(s) => assert_eq!(s, "45.67"),
            _ => panic!("Expected text value"),
        }
        Ok(())
    }

    #[test]
    fn serialize_params_mixed_types() -> Result<()> {
        use rusqlite::types::Null;
        let db = setup_db()?;
        
        // Test with mixed types using Vec<Box<dyn ToSql>>
        let params: Vec<Box<dyn ToSql>> = vec![
            Box::new("text_value"),
            Box::new(42i32),
            Box::new(3.14f64),
            Box::new(Null),
        ];
        let values = QuerySubscription::serialize_params(params)?;
        
        assert_eq!(values.len(), 4);
        match &values[0] {
            Value::Text(s) => assert_eq!(s, "text_value"),
            _ => panic!("Expected text value"),
        }
        match &values[1] {
            Value::Integer(i) => assert_eq!(*i, 42),
            _ => panic!("Expected integer value"),
        }
        match &values[2] {
            Value::Real(r) => assert!((r - 3.14).abs() < f64::EPSILON),
            _ => panic!("Expected real value"),
        }
        match &values[3] {
            Value::Null => {},
            _ => panic!("Expected null value"),
        }
        Ok(())
    }

    #[test]
    fn serialize_deserialize_params_roundtrip() -> Result<()> {
        let db = setup_db()?;
        
        // Test round-trip with various types
        let original_params: Vec<Box<dyn ToSql>> = vec![
            Box::new("test_string"),
            Box::new(42i32),
            Box::new(3.14159f64),
            Box::new(true),
        ];
        
        // Serialize
        let serialized = QuerySubscription::serialize_params(original_params)?;
        
        // Deserialize
        let deserialized = QuerySubscription::deserialize_params(&serialized);
        
        // Check that we got the same values back
        assert_eq!(serialized.len(), deserialized.len());
        assert_eq!(serialized, deserialized);
        Ok(())
    }

    #[test]
    fn params_work_with_actual_query() -> Result<()> {
        let db = setup_db()?;
        
        // Insert test data
        let artist = db.save(&Artist { 
            name: "Test Artist".to_string(), 
            ..Default::default() 
        })?;
        
        // Test parameters that we'll serialize/deserialize
        let original_params = vec!["Test Artist"];
        
        // Serialize parameters
        let serialized = QuerySubscription::serialize_params(original_params)?;
        
        // Deserialize parameters
        let deserialized = QuerySubscription::deserialize_params(&serialized);
        
        // Use the deserialized parameters in an actual query
        let results: Vec<Artist> = db.query(
            "SELECT * FROM Artist WHERE name = ?", 
            rusqlite::params_from_iter(deserialized)
        )?;
        
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "Test Artist");
        assert_eq!(results[0].id, artist.id);
        Ok(())
    }

    #[test]
    fn params_handle_nulls_correctly() -> Result<()> {
        use rusqlite::types::Null;
        let db = setup_db()?;
        
        // Test with null values
        let original_params: Vec<Box<dyn ToSql>> = vec![
            Box::new("some_text"),
            Box::new(Null),
            Box::new(123i32),
        ];
        
        // Serialize
        let serialized = QuerySubscription::serialize_params(original_params)?;
        
        // Check that null is preserved
        assert_eq!(serialized.len(), 3);
        match &serialized[1] {
            Value::Null => {}, // Expected
            _ => panic!("Expected null value to be preserved"),
        }
        
        // Deserialize
        let deserialized = QuerySubscription::deserialize_params(&serialized);
        
        // Verify round-trip
        assert_eq!(serialized, deserialized);
        Ok(())
    }

    #[test]
    fn empty_params_work() -> Result<()> {
        let db = setup_db()?;
        
        // Test empty parameters
        let empty_vec: Vec<&str> = vec![];
        let serialized = QuerySubscription::serialize_params(empty_vec)?;
        let deserialized = QuerySubscription::deserialize_params(&serialized);
        
        assert_eq!(serialized.len(), 0);
        assert_eq!(deserialized.len(), 0);
        
        // Should work with queries that don't need parameters
        let results: Vec<Artist> = db.query("SELECT * FROM Artist", rusqlite::params_from_iter(deserialized))?;
        assert_eq!(results.len(), 0); // No data inserted yet
        Ok(())
    }
    
    #[derive(Serialize, Deserialize, Default, Debug)]
    pub struct Artist {
        pub id: String,
        pub name: String,
        pub summary: Option<String>,
    }
}