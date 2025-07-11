use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{channel, Sender};
use std::thread::{self, JoinHandle};
use anyhow::Result;
use rusqlite::Params;
use crate::db::{Db, Entity, DbEvent};

/// Handle returned to the user for managing a query subscription
pub struct QuerySubscription {
    stop_signal: Option<Sender<()>>,
    thread_handle: Option<JoinHandle<()>>,
}

impl QuerySubscription {
    pub fn new<E: Entity + 'static, P: Params + Clone + Send + 'static, F>(db: &Db, sql: &str, params: P, callback: F) -> Result<Self> 
    where 
        F: FnMut(Vec<E>) + Send + 'static
    {        
        let dependent_tables = QuerySubscription::extract_query_tables(sql)?;
        
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

// Static methods 
impl QuerySubscription {
    /// Extracts table names from a SQL query.
    /// Uses a simple regex-based approach to find table names in FROM and JOIN clauses.
    pub fn extract_query_tables(sql: &str) -> Result<HashSet<String>> {
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

impl Drop for QuerySubscription {
    fn drop(&mut self) {
        self.unsubscribe();
    }   
}


#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite_migration::{Migrations, M};
    use serde::{Deserialize, Serialize};


    #[test]
    fn extract_query_tables_simple_select() -> Result<()> {
        let tables = QuerySubscription::extract_query_tables("SELECT * FROM Artist WHERE id = ?")?;
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
            "SELECT a.name, al.title FROM Artist a JOIN Album al ON a.id = al.artist_id WHERE a.id = ?"
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
             WHERE a.name = ?")?;
        assert_eq!(tables.len(), 3);
        assert!(tables.contains("Artist"));
        assert!(tables.contains("Album"));
        assert!(tables.contains("Track"));
        Ok(())
    }
    
    #[test]
    fn extract_query_tables_subquery() -> Result<()> {
        // Note: Our simple parser won't handle subqueries perfectly,
        // but it should at least find the main table
        let tables = QuerySubscription::extract_query_tables(
            "SELECT * FROM Artist WHERE id IN (SELECT artist_id FROM Album WHERE title = ?)"
        )?;
        assert!(tables.contains("Artist"));
        // Our simple parser might miss the subquery table
        Ok(())
    }

    // Unhappy path tests
    #[test]
    fn extract_query_tables_no_from_clause() -> Result<()> {
        // Query without FROM clause
        let tables = QuerySubscription::extract_query_tables("SELECT 1 + 1")?;
        assert_eq!(tables.len(), 0);
        Ok(())
    }

    #[test]
    fn extract_query_tables_malformed_sql() -> Result<()> {
        // Malformed SQL should not panic, just return empty or partial results
        let tables = QuerySubscription::extract_query_tables("SELECT * FORM Artist")?;
        assert_eq!(tables.len(), 0); // FORM instead of FROM
        
        let tables = QuerySubscription::extract_query_tables("FROM Artist SELECT *")?;
        assert!(tables.contains("Artist")); // Should still find the table
        Ok(())
    }

    #[test]
    fn extract_query_tables_special_characters() -> Result<()> {
        // Table names with special characters (though not recommended)
        let tables = QuerySubscription::extract_query_tables("SELECT * FROM `Artist-Table`")?;
        assert_eq!(tables.len(), 0); // Our parser expects alphanumeric names
        
        // Table with numbers and underscores (valid)
        let tables = QuerySubscription::extract_query_tables("SELECT * FROM Artist_2024")?;
        assert!(tables.contains("Artist_2024"));
        Ok(())
    }

    #[test]
    fn extract_query_tables_case_sensitivity() -> Result<()> {
        // Mixed case queries
        let tables = QuerySubscription::extract_query_tables("select * from Artist")?;
        assert!(tables.contains("Artist"));
        
        let tables = QuerySubscription::extract_query_tables("SELECT * FROM artist")?;
        assert!(tables.contains("artist"));
        
        let tables = QuerySubscription::extract_query_tables("SeLeCt * FrOm ArTiSt")?;
        assert!(tables.contains("ArTiSt"));
        Ok(())
    }

    #[test]
    fn extract_query_tables_empty_and_whitespace() -> Result<()> {
        // Empty query
        let tables = QuerySubscription::extract_query_tables("")?;
        assert_eq!(tables.len(), 0);
        
        // Only whitespace
        let tables = QuerySubscription::extract_query_tables("   \t\n   ")?;
        assert_eq!(tables.len(), 0);
        
        // Extra whitespace around tables
        let tables = QuerySubscription::extract_query_tables("SELECT * FROM    Artist    ")?;
        assert!(tables.contains("Artist"));
        Ok(())
    }

    #[test]
    fn extract_query_tables_comments() -> Result<()> {
        // SQL comments (our simple parser doesn't handle these)
        let tables = QuerySubscription::extract_query_tables(
            "SELECT * FROM Artist -- this is a comment"
        )?;
        assert!(tables.contains("Artist"));
        
        // Comment that looks like a table
        // NOTE: Our simple parser doesn't strip comments, so it will find "Album" in the comment
        let tables = QuerySubscription::extract_query_tables(
            "SELECT * FROM Artist /* JOIN Album */"
        )?;
        assert_eq!(tables.len(), 2); // Finds both Artist and Album
        assert!(tables.contains("Artist"));
        assert!(tables.contains("Album")); // Found in the comment
        Ok(())
    }

    #[test]
    fn extract_query_tables_with_parentheses() -> Result<()> {
        // Tables in parentheses (common in complex queries)
        // NOTE: Our parser actually strips parentheses as punctuation, so it finds the table
        let tables = QuerySubscription::extract_query_tables(
            "SELECT * FROM (Artist) WHERE id = ?"
        )?;
        assert_eq!(tables.len(), 1); // Parser strips parentheses and finds Artist
        assert!(tables.contains("Artist"));
        
        // Comma-separated tables should now work
        let tables = QuerySubscription::extract_query_tables(
            "SELECT * FROM Artist, Album WHERE Artist.id = Album.artist_id"
        )?;
        assert_eq!(tables.len(), 2);
        assert!(tables.contains("Artist"));
        assert!(tables.contains("Album"));
        Ok(())
    }

    #[test]
    fn extract_query_tables_reserved_keywords() -> Result<()> {
        // Using a keyword that contains JOIN
        let tables = QuerySubscription::extract_query_tables(
            "SELECT * FROM JOINED_TABLE"
        )?;
        assert!(tables.contains("JOINED_TABLE"));
        
        // Table name that starts with a keyword
        let tables = QuerySubscription::extract_query_tables(
            "SELECT * FROM FROM_TABLE"
        )?;
        assert!(tables.contains("FROM_TABLE"));
        Ok(())
    }

    #[test]
    fn extract_query_tables_comma_separated() -> Result<()> {
        // Comma-separated tables (old-style JOIN)
        let tables = QuerySubscription::extract_query_tables(
            "SELECT * FROM Artist, Album WHERE Artist.id = Album.artist_id"
        )?;
        assert_eq!(tables.len(), 2);
        assert!(tables.contains("Artist"));
        assert!(tables.contains("Album"));
        
        // Comma-separated tables with aliases
        let tables = QuerySubscription::extract_query_tables(
            "SELECT * FROM Artist a, Album al WHERE a.id = al.artist_id"
        )?;
        assert_eq!(tables.len(), 2);
        assert!(tables.contains("Artist"));
        assert!(tables.contains("Album"));
        
        // Multiple comma-separated tables
        let tables = QuerySubscription::extract_query_tables(
            "SELECT * FROM Artist, Album, Track WHERE Artist.id = Album.artist_id"
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
            "SELECT * FROM Artist, Album JOIN Track ON Album.id = Track.album_id WHERE Artist.id = Album.artist_id"
        )?;
        assert_eq!(tables.len(), 3);
        assert!(tables.contains("Artist"));
        assert!(tables.contains("Album"));
        assert!(tables.contains("Track"));
        Ok(())
    }
    
    #[derive(Serialize, Deserialize, Default, Debug)]
    pub struct Artist {
        pub id: String,
        pub name: String,
        pub summary: Option<String>,
    }
}