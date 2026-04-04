use std::time::{Duration, SystemTime};

use crate::resp::RespKind;

pub struct StoreEntry {
    value: RespKind,
    insertion_time: SystemTime,
    expiry: Option<Duration>,
}

impl StoreEntry {
    pub fn new(value: RespKind, expiry: Duration) -> Self {
        Self {
            value,
            insertion_time: SystemTime::now(),
            expiry: if expiry == Duration::MAX {
                None
            } else {
                Some(expiry)
            },
        }
    }

    pub fn value(&self) -> &RespKind {
        &self.value
    }

    pub fn value_mut(&mut self) -> &mut RespKind {
        &mut self.value
    }

    pub fn is_expired(&self) -> bool {
        match self.expiry {
            Some(duration) => {
                SystemTime::now()
                    .duration_since(self.insertion_time)
                    .unwrap_or(Duration::MAX)
                    > duration
            }
            None => false,
        }
    }
}
