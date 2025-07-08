use crate::sync::SyncTarget;

pub struct Sync {

}

impl Sync {
    pub fn new(target: Box<dyn SyncTarget>) -> anyhow::Result<Sync> {
        Ok(Sync {
            
        })
    }
}