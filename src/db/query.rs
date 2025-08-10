use std::collections::{HashSet, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{channel, Sender};
use std::thread::{self, JoinHandle};
use anyhow::Result;
use rusqlite::Params;
use serde::Serialize;
use crate::db::{Db, Entity, DbEvent, sql_parser};

/// Handle returned to the user for managing a query subscription
#[derive(Clone)]
pub struct QuerySubscription {
    stop_signal: Option<Sender<()>>,
    refresh_signal: Option<Sender<()>>,
    thread_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl QuerySubscription {
    fn calculate_hash<E: Serialize>(results: &[E]) -> u64 {
        let mut hasher = DefaultHasher::new();
        // Serialize the results to ensure we capture all data
        if let Ok(serialized) = serde_json::to_string(results) {
            serialized.hash(&mut hasher);
        }
        hasher.finish()
    }
    
    fn execute_callback<E, F>(callback: &Arc<Mutex<F>>, results: Vec<E>) 
    where 
        F: FnMut(Vec<E>) + Send
    {
        if let Ok(mut cb) = callback.lock() {
            cb(results);
        }
    }
    
    fn execute_query_and_callback_with_dedup<E: Entity, P: Params, F>(
        db: &Db, 
        sql: &str, 
        params: P, 
        callback: &Arc<Mutex<F>>,
        last_hash: &Arc<Mutex<Option<u64>>>
    ) 
    where 
        F: FnMut(Vec<E>) + Send
    {
        match db.query::<E, _>(sql, params) {
            Ok(results) => {
                let new_hash = Self::calculate_hash(&results);
                
                // Check if results have changed
                let should_notify = {
                    let mut last = last_hash.lock().unwrap();
                    if *last != Some(new_hash) {
                        *last = Some(new_hash);
                        true
                    } else {
                        false
                    }
                };
                
                if should_notify {
                    Self::execute_callback(callback, results);
                }
            },
            Err(e) => eprintln!("Error executing query: {:#}", e),
        }
    }
    
    fn monitor_thread<E: Entity + 'static, P: Params + Clone + Send + 'static, F>(
        db: Db,
        sql: String,
        params: P,
        tables: HashSet<String>,
        callback: Arc<Mutex<F>>,
        stop_rx: std::sync::mpsc::Receiver<()>,
        refresh_rx: std::sync::mpsc::Receiver<()>,
        last_hash: Arc<Mutex<Option<u64>>>,
    ) 
    where 
        F: FnMut(Vec<E>) + Send + 'static
    {
        let event_rx = db.subscribe();
        
        loop {
            // Check for stop signal
            if stop_rx.try_recv().is_ok() {
                break;
            }
            
            // TODO should be using crossbeam::select!() or something
            if refresh_rx.try_recv().is_ok() {
                Self::execute_query_and_callback_with_dedup::<E, _, F>(&db, &sql, params.clone(), &callback, &last_hash);
            }

            // Check for database events (with timeout to allow periodic stop checks)
            // TODO I think we can drop the timeout by ensuring the sender gets dropped
            // when the subscription is closed. Probably simplifies a lot of this.
            match event_rx.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(event) => {
                    // Check if this event affects our query
                    let table_name = match &event {
                        DbEvent::Insert(table, _) | DbEvent::Update(table, _) => table,
                    };
                    
                    if tables.contains(table_name) {
                        Self::execute_query_and_callback_with_dedup::<E, _, F>(&db, &sql, params.clone(), &callback, &last_hash);
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
    }

    pub fn new<E: Entity + 'static, P: Params + Clone + Send + 'static, F>(db: &Db, sql: &str, params: P, callback: F) -> Result<Self> 
    where 
        F: FnMut(Vec<E>) + Send + 'static
    {        
        let dependent_tables = sql_parser::extract_query_tables(sql)?;
        
        // Wrap the callback in Arc<Mutex<>> for thread safety
        let callback = Arc::new(Mutex::new(callback));
        
        // Track the hash of the last results
        let last_hash = Arc::new(Mutex::new(None::<u64>));
        
        // Run the query initially to provide immediate results
        let initial_results: Vec<E> = db.query(sql, params.clone())?;
        let initial_hash = Self::calculate_hash(&initial_results);
        *last_hash.lock().unwrap() = Some(initial_hash);
        Self::execute_callback(&callback, initial_results);
        
        // Create stop signal channel
        let (stop_tx, stop_rx) = channel::<()>();
        let (refresh_tx, refresh_rx) = channel::<()>();
        
        // Clone values needed for the thread
        let db_clone = db.clone();
        let sql_clone = sql.to_string();
        let params_clone = params.clone();
        let tables_clone = dependent_tables.clone();
        let callback_clone = callback.clone();
        let last_hash_clone = last_hash.clone();
        
        // Create the monitoring thread
        let thread_handle = thread::spawn(move || {
            Self::monitor_thread(db_clone, sql_clone, params_clone, tables_clone, callback_clone, stop_rx, refresh_rx, last_hash_clone);
        });
        
        Ok(QuerySubscription {
            stop_signal: Some(stop_tx),
            thread_handle: Arc::new(Mutex::new(Some(thread_handle))),
            refresh_signal: Some(refresh_tx),
        })
    }

    pub fn unsubscribe(&mut self) {
        // Send stop signal to the thread
        if let Some(stop_signal) = self.stop_signal.take() {
            let _ = stop_signal.send(()); // Ignore error if receiver already dropped
        }
        
        // Wait for the thread to finish
        if let Ok(mut guard) = self.thread_handle.lock() {
            if let Some(handle) = guard.take() {
                let _ = handle.join(); // Ignore error if thread panicked
            }
        }
    }
}

impl QuerySubscription {
    pub fn refresh(&self) {
        if let Some(refresh_tx) = &self.refresh_signal {
            let _ = refresh_tx.send(());
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
    fn test_query_subscription_with_proper_parser() -> Result<()> {
        // Test that the new parser is being used correctly in QuerySubscription
        let db = Db::open_memory()?;
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL, summary TEXT);"),
            M::up("CREATE TABLE Album (id TEXT PRIMARY KEY, title TEXT NOT NULL, artist_id TEXT);"),
        ]);
        db.migrate(&migrations)?;
        
        // Insert some test data
        db.save(&Artist {
            id: "1".to_string(),
            name: "Test Artist".to_string(),
            summary: None,
        })?;
        
        // Create a subscription with a complex query that the new parser handles well
        let results = Arc::new(Mutex::new(Vec::new()));
        let results_clone = results.clone();
        
        let mut subscription = QuerySubscription::new::<Artist, _, _>(
            &db,
            "SELECT * FROM Artist WHERE id IN (SELECT artist_id FROM Album)",
            (),
            move |data: Vec<Artist>| {
                if let Ok(mut r) = results_clone.lock() {
                    *r = data;
                }
            }
        )?;
        
        // Clean up
        subscription.unsubscribe();
        Ok(())
    }

    #[test]
    fn test_query_subscription_refresh() -> Result<()> {
        let db = Db::open_memory()?;
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL, summary TEXT);"),
        ]);
        db.migrate(&migrations)?;
        
        db.save(&Artist {
            id: "1".to_string(),
            name: "Artist 1".to_string(),
            summary: None,
        })?;
        
        let counter = Arc::new(Mutex::new(0));
        let counter_clone = counter.clone();
        
        let subscription = QuerySubscription::new::<Artist, _, _>(
            &db,
            "SELECT * FROM Artist",
            (),
            move |_data: Vec<Artist>| {
                if let Ok(mut c) = counter_clone.lock() {
                    *c += 1;
                }
            }
        )?;
        
        // Initial query should have run once
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(*counter.lock().unwrap(), 1);
        
        // Refresh with same data should NOT trigger another callback due to deduplication
        subscription.refresh();
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(*counter.lock().unwrap(), 1); // Should still be 1
        
        // But changing data should trigger the callback
        db.save(&Artist {
            id: "2".to_string(),
            name: "Artist 2".to_string(),
            summary: None,
        })?;
        
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert_eq!(*counter.lock().unwrap(), 2); // Now should be 2
        
        Ok(())
    }
    
    #[test]
    fn test_query_subscription_deduplication() -> Result<()> {
        let db = Db::open_memory()?;
        let migrations = Migrations::new(vec![
            M::up("CREATE TABLE Artist (id TEXT PRIMARY KEY, name TEXT NOT NULL, summary TEXT);"),
        ]);
        db.migrate(&migrations)?;
        
        db.save(&Artist {
            id: "1".to_string(),
            name: "Artist 1".to_string(),
            summary: None,
        })?;
        
        let counter = Arc::new(Mutex::new(0));
        let counter_clone = counter.clone();
        
        let subscription = QuerySubscription::new::<Artist, _, _>(
            &db,
            "SELECT * FROM Artist",
            (),
            move |_data: Vec<Artist>| {
                if let Ok(mut c) = counter_clone.lock() {
                    *c += 1;
                }
            }
        )?;
        
        // Initial query should have run once
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(*counter.lock().unwrap(), 1);
        
        // Refreshing without any data changes should NOT trigger the callback again
        subscription.refresh();
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(*counter.lock().unwrap(), 1); // Should still be 1
        
        // Another refresh should also not trigger
        subscription.refresh();
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(*counter.lock().unwrap(), 1); // Should still be 1
        
        // Adding new data should trigger the callback
        db.save(&Artist {
            id: "2".to_string(),
            name: "Artist 2".to_string(),
            summary: None,
        })?;
        
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert_eq!(*counter.lock().unwrap(), 2); // Should now be 2
        
        // Refresh after the data change should not trigger again (same results)
        subscription.refresh();
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(*counter.lock().unwrap(), 2); // Should still be 2
        
        Ok(())
    }
    
    #[derive(Serialize, Deserialize, Default, Debug)]
    pub struct Artist {
        pub id: String,
        pub name: String,
        pub summary: Option<String>,
    }
}