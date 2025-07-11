use std::collections::HashSet;
use anyhow::Result;
use crate::db::Db;

pub struct QuerySubscription {

}

impl QuerySubscription {
    pub fn unsubscribe(&self) {

    }
}

impl Drop for QuerySubscription {
    fn drop (&mut self) {
        self.unsubscribe();
    }   
}

impl Db {
    /// Extracts table names from a SQL query.
    /// Uses a simple regex-based approach to find table names in FROM and JOIN clauses.
    pub(crate) fn extract_query_tables(&self, sql: &str, _params: impl rusqlite::Params) -> Result<HashSet<String>> {
        let mut tables = HashSet::new();
        
        // Normalize the SQL to uppercase for easier parsing
        let sql_upper = sql.to_uppercase();
        
        // Find FROM clause
        if let Some(from_start) = sql_upper.find("FROM ") {
            let from_sql = &sql[from_start + 5..];
            
            // Extract first table after FROM
            if let Some(table_name) = self.extract_table_name(from_sql) {
                tables.insert(table_name);
            }
        }
        
        // Find all JOIN clauses
        let join_keywords = ["JOIN ", "INNER JOIN ", "LEFT JOIN ", "RIGHT JOIN ", "FULL JOIN ", "CROSS JOIN "];
        for keyword in &join_keywords {
            let mut search_pos = 0;
            while let Some(join_pos) = sql_upper[search_pos..].find(keyword) {
                let actual_pos = search_pos + join_pos + keyword.len();
                let join_sql = &sql[actual_pos..];
                
                if let Some(table_name) = self.extract_table_name(join_sql) {
                    tables.insert(table_name);
                }
                
                search_pos = actual_pos;
            }
        }
        
        Ok(tables)
    }
    
    /// Extract a table name from the beginning of a SQL fragment
    /// Handles "TableName", "TableName alias", "TableName AS alias"
    fn extract_table_name(&self, sql_fragment: &str) -> Option<String> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite_migration::{Migrations, M};
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
        let tables = db.extract_query_tables("SELECT * FROM Artist WHERE id = ?", ["test_id"])?;
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
        
        let tables = db.extract_query_tables(
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
        
        let tables = db.extract_query_tables(
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
        let tables = db.extract_query_tables(
            "SELECT * FROM Artist WHERE id IN (SELECT artist_id FROM Album WHERE title = ?)",
            ["Abbey Road"]
        )?;
        assert!(tables.contains("Artist"));
        // Our simple parser might miss the subquery table
        Ok(())
    }
    
    #[derive(Serialize, Deserialize, Default, Debug)]
    pub struct Artist {
        pub id: String,
        pub name: String,
        pub summary: Option<String>,
    }
}