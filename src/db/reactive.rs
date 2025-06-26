use std::sync::Arc;

use uuid::Uuid;

use super::core::Db;
use super::types::{Entity, QueryResult, QuerySubscription, QuerySubscriber, QueryObserver};

impl Db {
    pub fn subscribe_to_query<T: Entity + Send + 'static>(
        &self,
        sql: &str,
        params: &[&dyn rusqlite::ToSql],
    ) -> anyhow::Result<QuerySubscriber<T>> {
        // Generate unique subscription ID
        let subscription_id = Uuid::now_v7().to_string();

        // Extract table dependencies using EXPLAIN QUERY PLAN
        let dependent_tables = self.extract_table_dependencies(sql, params)?;

        // Execute the query to get initial results
        let initial_results: Vec<T> = self.query(sql, params)?;
        let initial_keys = self.extract_keys_from_results(&initial_results)?;
        let initial_hash = self.calculate_result_hash(&initial_results)?;

        // Serialize parameters for storage
        let serialized_params: Vec<String> = params
            .iter()
            .map(|p| self.serialize_tosql_param(p))
            .collect::<Result<Vec<_>, _>>()?;

        // Create subscription
        let subscription = QuerySubscription {
            id: subscription_id.clone(),
            sql: sql.to_string(),
            params: serialized_params,
            dependent_tables,
            last_result_keys: initial_keys.clone(),
            last_result_hash: initial_hash,
        };

        // Store subscription
        {
            let mut subscriptions = self
                .subscriptions
                .write()
                .map_err(|_| anyhow::anyhow!("Failed to acquire write lock on subscriptions"))?;
            subscriptions.insert(subscription_id.clone(), subscription);
        }

        // Create receiver for this subscription
        let rx = self.query_notifier.observer();

        // Send initial result
        let initial_query_result = QueryResult {
            data: initial_results,
            keys: initial_keys,
            hash: initial_hash,
        };

        // Convert to JSON for the generic notifier
        let initial_notification = serde_json::json!({
            "subscription_id": subscription_id,
            "type": "initial",
            "result": serde_json::to_value(&initial_query_result)?
        });

        self.query_notifier.notify(initial_notification);

        // Filter and convert the generic receiver to our specific type
        let (filtered_tx, filtered_rx) = std::sync::mpsc::channel::<QueryResult<T>>();
        let target_subscription_id = subscription_id.clone();

        std::thread::spawn(move || {
            for notification in rx {
                if let Some(obj) = notification.as_object() {
                    if let Some(sub_id) = obj.get("subscription_id").and_then(|v| v.as_str()) {
                        if sub_id == target_subscription_id {
                            if let Some(result_value) = obj.get("result") {
                                if let Ok(query_result) =
                                    serde_json::from_value::<QueryResult<T>>(result_value.clone())
                                {
                                    let _ = filtered_tx.send(query_result);
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(QuerySubscriber {
            subscription_id,
            db: Arc::downgrade(&self.subscriptions),
            _receiver: filtered_rx,
        })
    }

    pub fn observe_query<T: Entity + Send + 'static>(
        &self,
        sql: &str,
        params: &[&dyn rusqlite::ToSql],
        mut callback: impl FnMut(QueryResult<T>) + Send + 'static,
    ) -> anyhow::Result<QueryObserver> {
        // Generate unique subscription ID
        let subscription_id = Uuid::now_v7().to_string();

        // Extract table dependencies using EXPLAIN QUERY PLAN
        let dependent_tables = self.extract_table_dependencies(sql, params)?;

        // Execute the query to get initial results
        let initial_results: Vec<T> = self.query(sql, params)?;
        let initial_keys = self.extract_keys_from_results(&initial_results)?;
        let initial_hash = self.calculate_result_hash(&initial_results)?;

        // Serialize parameters for storage
        let serialized_params: Vec<String> = params
            .iter()
            .map(|p| self.serialize_tosql_param(p))
            .collect::<Result<Vec<_>, _>>()?;

        // Create subscription
        let subscription = QuerySubscription {
            id: subscription_id.clone(),
            sql: sql.to_string(),
            params: serialized_params,
            dependent_tables,
            last_result_keys: initial_keys.clone(),
            last_result_hash: initial_hash,
        };

        // Store subscription
        {
            let mut subscriptions = self
                .subscriptions
                .write()
                .map_err(|_| anyhow::anyhow!("Failed to acquire write lock on subscriptions"))?;
            subscriptions.insert(subscription_id.clone(), subscription);
        }

        // Create initial result and deliver it immediately
        let initial_query_result = QueryResult {
            data: initial_results,
            keys: initial_keys,
            hash: initial_hash,
        };

        // Deliver initial result
        callback(initial_query_result);

        // Set up ongoing observation for future changes
        let target_subscription_id = subscription_id.clone();
        self.query_notifier.observe(move |notification| {
            if let Some(obj) = notification.as_object() {
                if let Some(sub_id) = obj.get("subscription_id").and_then(|v| v.as_str()) {
                    if sub_id == target_subscription_id {
                        if let Some(result_value) = obj.get("result") {
                            if let Ok(query_result) =
                                serde_json::from_value::<QueryResult<T>>(result_value.clone())
                            {
                                callback(query_result);
                            }
                        }
                    }
                }
            }
        });

        // Return cleanup handle
        Ok(QueryObserver {
            subscription_id,
            db: Arc::downgrade(&self.subscriptions),
        })
    }

    pub(crate) fn notify_query_subscribers(
        &self,
        changed_table: &str,
        changed_key: &str,
    ) -> anyhow::Result<()> {
        // Get a snapshot of affected subscriptions
        let affected_subscriptions = {
            let subs = self
                .subscriptions
                .read()
                .map_err(|_| anyhow::anyhow!("Failed to acquire read lock on subscriptions"))?;
            subs.iter()
                .filter(|(_, subscription)| {
                    subscription.dependent_tables.contains(changed_table)
                        || subscription
                            .dependent_tables
                            .contains(&changed_table.to_uppercase())
                        || subscription.last_result_keys.contains(changed_key)
                })
                .map(|(id, sub)| (id.clone(), sub.clone()))
                .collect::<Vec<_>>()
        }; // Lock released here

        // Process affected subscriptions without holding the subscription lock
        for (subscription_id, subscription) in affected_subscriptions {
            // Re-execute the query to check for changes
            self.check_and_notify_subscription(&subscription_id, &subscription)?;
        }

        Ok(())
    }

    fn check_and_notify_subscription(
        &self,
        subscription_id: &str,
        subscription: &QuerySubscription,
    ) -> anyhow::Result<()> {
        // Re-execute the query to get current results
        let params = self.deserialize_params(&subscription.params)?;
        let results: Vec<serde_json::Value> =
            self.query_as_json_with_params(&subscription.sql, &params)?;

        // Calculate new hash and keys
        let new_hash = self.calculate_result_hash_from_json(&results)?;
        let new_keys = self.extract_keys_from_json_results(&results)?;

        // Check if results have changed (compare with subscription state)
        if new_hash != subscription.last_result_hash || new_keys != subscription.last_result_keys {
            // Update subscription with new state - acquire lock briefly
            let mut subscriptions = self.subscriptions.write().map_err(|_| {
                anyhow::anyhow!("Failed to acquire write lock on subscriptions for update")
            })?;
            if let Some(sub) = subscriptions.get_mut(subscription_id) {
                sub.last_result_hash = new_hash;
                sub.last_result_keys = new_keys.clone();
            }
            drop(subscriptions); // Explicitly release lock

            // Create result object (generic JSON for now)
            let query_result = serde_json::json!({
                "data": results,
                "keys": new_keys.into_iter().collect::<Vec<_>>(),
                "hash": new_hash
            });

            // Send notification
            let notification = serde_json::json!({
                "subscription_id": subscription_id,
                "type": "update",
                "result": query_result
            });

            self.query_notifier.notify(notification);
        }

        Ok(())
    }

    pub fn get_subscription_count(&self) -> anyhow::Result<usize> {
        let subscriptions = self
            .subscriptions
            .read()
            .map_err(|_| anyhow::anyhow!("Failed to acquire read lock on subscriptions"))?;
        Ok(subscriptions.len())
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
    fn test_reactive_query_basic() -> anyhow::Result<()> {
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

        // Subscribe to a query
        let subscriber =
            db.subscribe_to_query::<Artist>("SELECT * FROM Artist WHERE name LIKE 'Metal%'", &[])?;

        // Collect initial result
        let initial_result = subscriber.recv_timeout(std::time::Duration::from_millis(100))?;
        assert_eq!(initial_result.data.len(), 0); // No matching artists initially

        // Save an artist that should trigger the query
        let _artist = db.save(&Artist {
            name: "Metallica".to_string(),
            disambiguation: Some("American metal band".to_string()),
            ..Default::default()
        })?;

        // Should receive a notification
        let updated_result = subscriber.recv_timeout(std::time::Duration::from_millis(1000))?;
        assert_eq!(updated_result.data.len(), 1);
        assert_eq!(updated_result.data[0].name, "Metallica");

        Ok(())
    }

    #[test]
    fn test_reactive_query_no_match() -> anyhow::Result<()> {
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

        // Subscribe to a specific query
        let subscriber =
            db.subscribe_to_query::<Artist>("SELECT * FROM Artist WHERE name = 'Metallica'", &[])?;

        // Collect initial result
        let initial_result = subscriber.recv_timeout(std::time::Duration::from_millis(100))?;
        assert_eq!(initial_result.data.len(), 0);

        // Save an artist that should trigger re-evaluation but result in same empty set
        let _artist = db.save(&Artist {
            name: "Iron Maiden".to_string(),
            disambiguation: Some("British metal band".to_string()),
            ..Default::default()
        })?;

        // Should receive a notification (table changed) but result set should still be empty
        let updated_result = subscriber.recv_timeout(std::time::Duration::from_millis(1000))?;
        assert_eq!(updated_result.data.len(), 0); // Still no matching artists

        Ok(())
    }

    #[test]
    fn test_subscription_management() -> anyhow::Result<()> {
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

        // Initially no subscriptions
        assert_eq!(db.get_subscription_count()?, 0);

        // Subscribe to a query
        let _subscriber = db.subscribe_to_query::<Artist>("SELECT * FROM Artist", &[])?;

        // Should have 1 subscription
        assert_eq!(db.get_subscription_count()?, 1);

        Ok(())
    }

    #[test]
    fn test_observe_query_callback() -> anyhow::Result<()> {
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

        // Use Arc<Mutex<Vec>> to collect results from callback
        let results = std::sync::Arc::new(std::sync::Mutex::new(Vec::<QueryResult<Artist>>::new()));
        let results_clone = results.clone();

        // Observe query with callback
        let _observer = db.observe_query::<Artist>(
            "SELECT * FROM Artist WHERE name LIKE ?",
            &[&"Metal%"],
            move |query_result| {
                if let Ok(mut r) = results_clone.lock() {
                    r.push(query_result);
                }
            },
        )?;

        // Give callback time to process initial result
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Should have received initial empty result
        {
            let r = results.lock().unwrap();
            assert_eq!(r.len(), 1);
            assert_eq!(r[0].data.len(), 0);
        }

        // Save an artist that matches the query
        let _artist = db.save(&Artist {
            name: "Metallica".to_string(),
            disambiguation: Some("American metal band".to_string()),
            ..Default::default()
        })?;

        // Give callback time to process the update
        std::thread::sleep(std::time::Duration::from_millis(50));

        // Should have received both initial and updated results
        {
            let r = results.lock().unwrap();
            assert_eq!(r.len(), 2);
            assert_eq!(r[0].data.len(), 0); // Initial empty result
            assert_eq!(r[1].data.len(), 1); // Updated result with Metallica
            assert_eq!(r[1].data[0].name, "Metallica");
        }

        Ok(())
    }

    #[test]
    fn test_subscription_cleanup_on_drop() -> anyhow::Result<()> {
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

        // Initially no subscriptions
        assert_eq!(db.get_subscription_count()?, 0);

        // Create subscription in inner scope
        {
            let _subscriber = db.subscribe_to_query::<Artist>("SELECT * FROM Artist", &[])?;
            // Should have 1 subscription
            assert_eq!(db.get_subscription_count()?, 1);
        } // subscriber drops here

        // Give cleanup time to happen
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Should be cleaned up
        assert_eq!(db.get_subscription_count()?, 0);

        Ok(())
    }

    #[test]
    fn test_observer_cleanup_on_drop() -> anyhow::Result<()> {
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

        // Initially no subscriptions
        assert_eq!(db.get_subscription_count()?, 0);

        // Create observer in inner scope
        {
            let _observer = db.observe_query::<Artist>(
                "SELECT * FROM Artist WHERE name LIKE ?",
                &[&"Metal%"],
                |_| {},
            )?;
            // Should have 1 subscription
            assert_eq!(db.get_subscription_count()?, 1);
        } // observer drops here

        // Give cleanup time to happen
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Should be cleaned up
        assert_eq!(db.get_subscription_count()?, 0);

        Ok(())
    }
}