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