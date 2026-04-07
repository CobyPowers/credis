use std::{
    collections::HashMap,
    fmt::Display,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::resp::RespKind;

#[derive(Debug, Default)]
pub struct Store {
    pub hash_map: HashMap<String, StoreEntry>,
    // Tracks last insertion id for all stream
    // entries to ensure O(1) time complexity
    pub stream_entry_insertion_map: HashMap<String, StreamEntryId>,
}

#[derive(Debug)]
pub enum StreamEntryIdError {
    ParseError,
    EqualZeroError,
    OldEntryError,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamEntryId {
    time_ms: u128, // Time since unix epoch
    index: u64,
}

impl StreamEntryId {
    fn from_query_id(query_id: &String) -> Result<Self, StreamEntryIdError> {
        let (time_ms, index) = if query_id == "*" {
            (
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Failed to compute millis since unix epoch")
                    .as_millis(),
                1,
            )
        } else {
            let id = query_id.replace('*', "1");
            match id.split_once('-') {
                Some((time_ms, index)) => match (time_ms.parse::<u128>(), index.parse::<u64>()) {
                    (Ok(time_ms), Ok(index)) => (time_ms, index),
                    _ => return Err(StreamEntryIdError::ParseError),
                },
                _ => return Err(StreamEntryIdError::ParseError),
            }
        };

        if time_ms > 0 || index > 0 {
            Ok(Self { time_ms, index })
        } else {
            Err(StreamEntryIdError::EqualZeroError)
        }
    }
}

impl Display for StreamEntryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(format!("{}-{}", self.time_ms, self.index).as_str())
    }
}

impl Store {
    pub fn sweep(&mut self) {
        let mut removable_keys = vec![];

        for (k, v) in self.hash_map.iter() {
            if v.is_expired() {
                removable_keys.push(k.clone());
            }
        }

        for k in removable_keys.iter() {
            self.hash_map.remove(k);
        }
    }

    pub fn get(&self, key: &String) -> Option<&RespKind> {
        self.hash_map.get(key).and_then(|entry| {
            if !entry.is_expired() {
                Some(entry.value())
            } else {
                None
            }
        })
    }

    pub fn remove(&mut self, key: &String) {
        self.hash_map.remove(key);
    }

    pub fn insert_string(&mut self, key: String, value: String, expiry: Duration) {
        let entry = StoreEntry::new(RespKind::BulkString(value), expiry);
        self.hash_map.insert(key, entry);
    }

    pub fn get_string(&self, key: &String) -> Option<&String> {
        self.hash_map
            .get(key)
            .and_then(|entry| match entry.value() {
                RespKind::BulkString(val) if !entry.is_expired() => Some(val),
                _ => None,
            })
    }

    pub fn insert_list_mut(&mut self, key: String, values: Vec<RespKind>) -> &mut Vec<RespKind> {
        let entry = StoreEntry::new(RespKind::Array(values), Duration::MAX);
        self.hash_map.insert(key.clone(), entry);
        match self.get_list_mut(&key) {
            Some(list) => list,
            _ => unreachable!(),
        }
    }

    pub fn get_list(&self, key: &String) -> Option<&Vec<RespKind>> {
        self.hash_map
            .get(key)
            .and_then(|entry| match entry.value() {
                RespKind::Array(list) => Some(list),
                _ => None,
            })
    }

    pub fn get_list_mut(&mut self, key: &String) -> Option<&mut Vec<RespKind>> {
        self.hash_map
            .get_mut(key)
            .and_then(|entry| match entry.value_mut() {
                RespKind::Array(list) => Some(list),
                _ => None,
            })
    }

    pub fn create_stream_mut(&mut self, key: String) -> &mut HashMap<String, RespKind> {
        let entry = StoreEntry::new(RespKind::Map(HashMap::new()), Duration::MAX);
        self.hash_map.insert(key.clone(), entry);
        match self.get_stream_mut(&key) {
            Some(map) => map,
            _ => unreachable!(),
        }
    }

    pub fn get_stream_mut(&mut self, key: &String) -> Option<&mut HashMap<String, RespKind>> {
        self.hash_map
            .get_mut(key)
            .and_then(|entry| match entry.value_mut() {
                RespKind::Map(map) => Some(map),
                _ => None,
            })
    }

    pub fn create_stream_entry_mut(
        &mut self,
        key: &String,
        query_id: &String,
    ) -> Result<(&mut HashMap<String, RespKind>, String), StreamEntryIdError> {
        let last_entry_id = self
            .stream_entry_insertion_map
            .get(key)
            .cloned()
            .unwrap_or_default();

        let stream = match self.get_stream_mut(key) {
            Some(map) => map,
            None => self.create_stream_mut(key.clone()),
        };

        let mut id = match StreamEntryId::from_query_id(query_id) {
            Ok(id) => id,
            Err(e) => return Err(e),
        };
        if id.time_ms == last_entry_id.time_ms {
            id.index = last_entry_id.index + 1;
        }
        let id_str = id.to_string();

        if id > last_entry_id {
            stream.insert(id_str.clone(), RespKind::Map(HashMap::new()));
            self.stream_entry_insertion_map.insert(key.clone(), id);

            match self.get_stream_entry_mut(key, &id_str) {
                Some(map) => Ok((map, id_str)),
                _ => unreachable!(),
            }
        } else {
            Err(StreamEntryIdError::OldEntryError)
        }
    }

    pub fn get_stream_entry_mut(
        &mut self,
        key: &String,
        id: &String,
    ) -> Option<&mut HashMap<String, RespKind>> {
        let stream = self.get_stream_mut(key)?;
        stream
            .get_mut(id)
            .and_then(|stream_entry| match stream_entry {
                RespKind::Map(map) => Some(map),
                _ => None,
            })
    }
}

#[derive(Debug)]
pub struct StoreEntry {
    value: RespKind,
    insertion_time: SystemTime,
    expiry_dur: Option<Duration>,
}

impl StoreEntry {
    pub fn new(value: RespKind, expiry: Duration) -> Self {
        Self {
            value,
            insertion_time: SystemTime::now(),
            expiry_dur: if expiry == Duration::MAX {
                None
            } else {
                Some(expiry)
            },
        }
    }

    pub fn set_value(&mut self, value: RespKind) {
        self.value = value;
    }

    pub fn value(&self) -> &RespKind {
        &self.value
    }

    pub fn value_mut(&mut self) -> &mut RespKind {
        &mut self.value
    }

    pub fn is_expired(&self) -> bool {
        match self.expiry_dur {
            Some(duration) => {
                SystemTime::now()
                    .duration_since(self.insertion_time)
                    .unwrap_or(Duration::MAX)
                    >= duration
            }
            None => false,
        }
    }
}
