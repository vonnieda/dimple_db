use std::collections::HashSet;
use anyhow::Result;
use sqlparser::ast::{Statement, TableFactor, TableWithJoins, Select, SetExpr, Query, Expr};
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;

/// Extracts all table names referenced in a SQL query
pub fn extract_query_tables(sql: &str) -> Result<HashSet<String>> {
    let dialect = SQLiteDialect {};
    let statements = Parser::parse_sql(&dialect, sql)?;
    
    let mut tables = HashSet::new();
    
    for statement in statements {
        extract_tables_from_statement(&statement, &mut tables);
    }
    
    Ok(tables)
}

fn extract_tables_from_statement(statement: &Statement, tables: &mut HashSet<String>) {
    match statement {
        Statement::Query(query) => extract_tables_from_query(query, tables),
        Statement::Insert(insert) => {
            // Extract table name from the INSERT statement
            if let Some(table_name) = insert.table_name.0.last() {
                tables.insert(table_name.value.clone());
            }
            // Check if there's a SELECT source
            if let Some(source) = &insert.source {
                extract_tables_from_set_expr(&source.body, tables);
            }
        },
        Statement::Update { table, from, .. } => {
            extract_tables_from_table_with_joins(&table, tables);
            if let Some(from) = from {
                extract_tables_from_table_with_joins(&from, tables);
            }
        },
        Statement::Delete(delete) => {
            // Extract tables from DELETE statement
            match &delete.from {
                sqlparser::ast::FromTable::WithFromKeyword(from_tables) => {
                    for table_with_joins in from_tables {
                        extract_tables_from_table_with_joins(table_with_joins, tables);
                    }
                },
                sqlparser::ast::FromTable::WithoutKeyword(from_tables) => {
                    for table_with_joins in from_tables {
                        extract_tables_from_table_with_joins(table_with_joins, tables);
                    }
                }
            }
            // Also check the USING clause if present
            if let Some(using) = &delete.using {
                for table_with_joins in using {
                    extract_tables_from_table_with_joins(table_with_joins, tables);
                }
            }
            // Check WHERE clause for subqueries
            if let Some(where_expr) = &delete.selection {
                extract_tables_from_expr(where_expr, tables);
            }
        },
        _ => {}
    }
}

fn extract_tables_from_query(query: &Query, tables: &mut HashSet<String>) {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            extract_tables_from_query(&cte.query, tables);
        }
    }
    
    extract_tables_from_set_expr(&query.body, tables);
}

fn extract_tables_from_set_expr(set_expr: &SetExpr, tables: &mut HashSet<String>) {
    match set_expr {
        SetExpr::Select(select) => extract_tables_from_select(select, tables),
        SetExpr::Query(query) => extract_tables_from_query(query, tables),
        SetExpr::SetOperation { left, right, .. } => {
            extract_tables_from_set_expr(left, tables);
            extract_tables_from_set_expr(right, tables);
        },
        _ => {}
    }
}

fn extract_tables_from_select(select: &Select, tables: &mut HashSet<String>) {
    // Extract from FROM clause
    for table_with_joins in &select.from {
        extract_tables_from_table_with_joins(table_with_joins, tables);
    }
    
    // Extract from WHERE clause (for subqueries)
    if let Some(where_expr) = &select.selection {
        extract_tables_from_expr(where_expr, tables);
    }
    
    // Extract from HAVING clause (for subqueries)
    if let Some(having_expr) = &select.having {
        extract_tables_from_expr(having_expr, tables);
    }
}

fn extract_tables_from_expr(expr: &Expr, tables: &mut HashSet<String>) {
    match expr {
        Expr::Subquery(query) | Expr::Exists { subquery: query, .. } | Expr::InSubquery { subquery: query, .. } => {
            extract_tables_from_query(query, tables);
        },
        Expr::BinaryOp { left, right, .. } => {
            extract_tables_from_expr(left, tables);
            extract_tables_from_expr(right, tables);
        },
        Expr::UnaryOp { expr, .. } => {
            extract_tables_from_expr(expr, tables);
        },
        Expr::Between { expr, low, high, .. } => {
            extract_tables_from_expr(expr, tables);
            extract_tables_from_expr(low, tables);
            extract_tables_from_expr(high, tables);
        },
        Expr::InList { expr, list, .. } => {
            extract_tables_from_expr(expr, tables);
            for item in list {
                extract_tables_from_expr(item, tables);
            }
        },
        Expr::Case { operand, conditions, results, else_result } => {
            if let Some(op) = operand {
                extract_tables_from_expr(op, tables);
            }
            for cond in conditions {
                extract_tables_from_expr(cond, tables);
            }
            for res in results {
                extract_tables_from_expr(res, tables);
            }
            if let Some(else_res) = else_result {
                extract_tables_from_expr(else_res, tables);
            }
        },
        _ => {}
    }
}

fn extract_tables_from_table_with_joins(table_with_joins: &TableWithJoins, tables: &mut HashSet<String>) {
    extract_tables_from_table_factor(&table_with_joins.relation, tables);
    
    for join in &table_with_joins.joins {
        extract_tables_from_table_factor(&join.relation, tables);
    }
}

fn extract_tables_from_table_factor(table_factor: &TableFactor, tables: &mut HashSet<String>) {
    match table_factor {
        TableFactor::Table { name, .. } => {
            // Get the table name without schema prefix
            if let Some(table_name) = name.0.last() {
                tables.insert(table_name.value.clone());
            }
        },
        TableFactor::Derived { subquery, .. } => {
            extract_tables_from_query(subquery, tables);
        },
        TableFactor::TableFunction { .. } => {
            // Table functions don't reference actual tables
        },
        TableFactor::NestedJoin { table_with_joins, .. } => {
            extract_tables_from_table_with_joins(table_with_joins, tables);
        },
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() -> Result<()> {
        let sql = "SELECT * FROM Artist WHERE id = ?";
        let tables = extract_query_tables(sql)?;
        assert_eq!(tables.len(), 1);
        assert!(tables.contains("Artist"));
        Ok(())
    }

    #[test]
    fn test_join_query() -> Result<()> {
        let sql = "SELECT a.name, al.title FROM Artist a JOIN Album al ON a.id = al.artist_id WHERE a.id = ?";
        let tables = extract_query_tables(sql)?;
        assert_eq!(tables.len(), 2);
        assert!(tables.contains("Artist"));
        assert!(tables.contains("Album"));
        Ok(())
    }

    #[test]
    fn test_multiple_joins() -> Result<()> {
        let sql = "SELECT a.name, al.title, t.title 
                   FROM Artist a 
                   LEFT JOIN Album al ON a.id = al.artist_id 
                   INNER JOIN Track t ON al.id = t.album_id 
                   WHERE a.name = ?";
        let tables = extract_query_tables(sql)?;
        assert_eq!(tables.len(), 3);
        assert!(tables.contains("Artist"));
        assert!(tables.contains("Album"));
        assert!(tables.contains("Track"));
        Ok(())
    }

    #[test]
    fn test_subquery() -> Result<()> {
        let sql = "SELECT * FROM Artist WHERE id IN (SELECT artist_id FROM Album WHERE title = ?)";
        let tables = extract_query_tables(sql)?;
        assert!(tables.contains("Artist"));
        assert!(tables.contains("Album"));
        Ok(())
    }

    #[test]
    fn test_comma_separated_tables() -> Result<()> {
        let sql = "SELECT * FROM Artist, Album WHERE Artist.id = Album.artist_id";
        let tables = extract_query_tables(sql)?;
        assert_eq!(tables.len(), 2);
        assert!(tables.contains("Artist"));
        assert!(tables.contains("Album"));
        Ok(())
    }

    #[test]
    fn test_with_cte() -> Result<()> {
        let sql = "WITH recent_albums AS (SELECT * FROM Album WHERE year > 2020) 
                   SELECT * FROM Artist a JOIN recent_albums r ON a.id = r.artist_id";
        let tables = extract_query_tables(sql)?;
        assert!(tables.contains("Artist"));
        assert!(tables.contains("Album"));
        Ok(())
    }

    #[test]
    fn test_union_query() -> Result<()> {
        let sql = "SELECT name FROM Artist UNION SELECT title as name FROM Album";
        let tables = extract_query_tables(sql)?;
        assert_eq!(tables.len(), 2);
        assert!(tables.contains("Artist"));
        assert!(tables.contains("Album"));
        Ok(())
    }

    #[test]
    fn test_insert_with_select() -> Result<()> {
        let sql = "INSERT INTO Artist_Copy SELECT * FROM Artist";
        let tables = extract_query_tables(sql)?;
        assert!(tables.contains("Artist_Copy"));
        assert!(tables.contains("Artist"));
        Ok(())
    }

    #[test]
    fn test_update_with_from() -> Result<()> {
        let sql = "UPDATE Artist SET name = 'Updated' FROM Album WHERE Artist.id = Album.artist_id";
        let tables = extract_query_tables(sql)?;
        assert!(tables.contains("Artist"));
        assert!(tables.contains("Album"));
        Ok(())
    }

    #[test]
    fn test_delete_with_join() -> Result<()> {
        let sql = "DELETE FROM Artist WHERE id IN (SELECT artist_id FROM Album WHERE year < 2000)";
        let tables = extract_query_tables(sql)?;
        assert!(tables.contains("Artist"));
        assert!(tables.contains("Album"));
        Ok(())
    }

    #[test]
    fn test_derived_table() -> Result<()> {
        let sql = "SELECT * FROM (SELECT * FROM Artist WHERE active = 1) as active_artists";
        let tables = extract_query_tables(sql)?;
        assert!(tables.contains("Artist"));
        Ok(())
    }

    #[test]
    fn test_nested_joins() -> Result<()> {
        let sql = "SELECT * FROM (Artist a JOIN Album al ON a.id = al.artist_id) JOIN Track t ON al.id = t.album_id";
        let tables = extract_query_tables(sql)?;
        assert_eq!(tables.len(), 3);
        assert!(tables.contains("Artist"));
        assert!(tables.contains("Album"));
        assert!(tables.contains("Track"));
        Ok(())
    }

    #[test]
    fn test_empty_query() -> Result<()> {
        let tables = extract_query_tables("")?;
        assert_eq!(tables.len(), 0);
        Ok(())
    }

    #[test]
    fn test_malformed_sql() {
        // This should return an error from the parser
        let result = extract_query_tables("SELECT * FORM Artist");
        assert!(result.is_err());
    }
}