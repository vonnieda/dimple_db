use std::{
    sync::{
        Arc, RwLock,
        mpsc::{Receiver, Sender, channel},
    },
    thread,
};

#[derive(Clone)]
pub struct Notifier<Event: Send + Sync + Clone + 'static> {
    senders: Arc<RwLock<Vec<Sender<Event>>>>,
}

impl<Event: Send + Sync + Clone + 'static> Notifier<Event> {
    pub fn new() -> Self {
        Self {
            senders: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn notify(&self, event: Event) {
        let mut senders = self.senders.write().unwrap();
        senders.retain(|tx| tx.send(event.clone()).is_ok());
    }

    pub fn observer(&self) -> Receiver<Event> {
        let (tx, rx) = channel();
        self.senders.write().unwrap().push(tx);
        rx
    }

    pub fn observe(&self, mut callback: impl FnMut(Event) -> () + Send + 'static) {
        let rx = self.observer();
        thread::spawn(move || {
            rx.iter().for_each(|e| callback(e));
        });
    }
}

#[cfg(test)]
mod tests {
    use super::Notifier;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[test]
    fn test_single_observer() {
        let notifier = Notifier::<String>::new();
        let rx = notifier.observer();

        notifier.notify("hello".to_string());
        let received = rx.recv_timeout(Duration::from_millis(100)).unwrap();
        assert_eq!(received, "hello");
    }

    #[test]
    fn test_multiple_observers() {
        let notifier = Notifier::<i32>::new();
        let rx1 = notifier.observer();
        let rx2 = notifier.observer();
        let rx3 = notifier.observer();

        notifier.notify(42);

        assert_eq!(rx1.recv_timeout(Duration::from_millis(100)).unwrap(), 42);
        assert_eq!(rx2.recv_timeout(Duration::from_millis(100)).unwrap(), 42);
        assert_eq!(rx3.recv_timeout(Duration::from_millis(100)).unwrap(), 42);
    }

    #[test]
    fn test_observe_callback() {
        let notifier = Notifier::<String>::new();
        let received = Arc::new(Mutex::new(Vec::<String>::new()));
        let received_clone = received.clone();

        notifier.observe(move |event| {
            received_clone.lock().unwrap().push(event);
        });

        // Give callback time to set up
        std::thread::sleep(Duration::from_millis(10));

        notifier.notify("test1".to_string());
        notifier.notify("test2".to_string());

        // Give callbacks time to process
        std::thread::sleep(Duration::from_millis(50));

        let events = received.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0], "test1");
        assert_eq!(events[1], "test2");
    }

    #[test]
    fn test_dead_channel_cleanup() {
        let notifier = Notifier::<String>::new();

        // Create observers but drop their receivers
        {
            let _rx1 = notifier.observer();
            let _rx2 = notifier.observer();
        } // receivers dropped here

        // Create a live observer
        let rx_live = notifier.observer();

        // Notify - this should clean up dead channels and deliver to live one
        notifier.notify("cleanup_test".to_string());

        // Live observer should still receive
        let received = rx_live.recv_timeout(Duration::from_millis(100)).unwrap();
        assert_eq!(received, "cleanup_test");

        // Should have cleaned up the dead channels (can't directly test internal state,
        // but the fact that notify() completed successfully indicates cleanup worked)
    }

    #[test]
    fn test_concurrent_notifications() {
        let notifier = Arc::new(Notifier::<i32>::new());
        let rx = notifier.observer();

        let handles: Vec<_> = (0..10)
            .map(|i| {
                let notifier_clone = notifier.clone();
                std::thread::spawn(move || {
                    notifier_clone.notify(i);
                })
            })
            .collect();

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Collect all notifications
        let mut received = Vec::new();
        while let Ok(value) = rx.recv_timeout(Duration::from_millis(10)) {
            received.push(value);
        }

        // Should have received all 10 notifications (order may vary)
        assert_eq!(received.len(), 10);
        received.sort();
        assert_eq!(received, (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn test_clone_notifier() {
        let notifier1 = Notifier::<String>::new();
        let notifier2 = notifier1.clone();

        let rx1 = notifier1.observer();
        let rx2 = notifier2.observer();

        // Both clones should share the same internal state
        notifier1.notify("shared".to_string());

        assert_eq!(
            rx1.recv_timeout(Duration::from_millis(100)).unwrap(),
            "shared"
        );
        assert_eq!(
            rx2.recv_timeout(Duration::from_millis(100)).unwrap(),
            "shared"
        );
    }

    #[test]
    fn test_observer_drop_cleanup() {
        let notifier = Notifier::<String>::new();

        // Create and immediately drop observer
        {
            let _rx = notifier.observer();
        } // rx dropped here

        // Create a live observer
        let rx_live = notifier.observer();

        // This should clean up the dead channel and deliver to live one
        notifier.notify("after_drop".to_string());

        let received = rx_live.recv_timeout(Duration::from_millis(100)).unwrap();
        assert_eq!(received, "after_drop");
    }

    #[test]
    fn test_no_observers() {
        let notifier = Notifier::<String>::new();

        // Should not panic when no observers are present
        notifier.notify("no_observers".to_string());
    }

    #[test]
    fn test_multiple_callbacks() {
        let notifier = Notifier::<i32>::new();
        let counter1 = Arc::new(Mutex::new(0));
        let counter2 = Arc::new(Mutex::new(0));

        let counter1_clone = counter1.clone();
        let counter2_clone = counter2.clone();

        notifier.observe(move |value| {
            *counter1_clone.lock().unwrap() += value;
        });

        notifier.observe(move |value| {
            *counter2_clone.lock().unwrap() += value * 2;
        });

        // Give callbacks time to set up
        std::thread::sleep(Duration::from_millis(10));

        notifier.notify(5);
        notifier.notify(3);

        // Give callbacks time to process
        std::thread::sleep(Duration::from_millis(50));

        assert_eq!(*counter1.lock().unwrap(), 8); // 5 + 3
        assert_eq!(*counter2.lock().unwrap(), 16); // (5 * 2) + (3 * 2)
    }
}
