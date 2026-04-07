use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::resp::{RespKind, ToRespValue};

#[derive(Debug)]
pub enum StreamIdError {
    ParseError,
    EqualZeroError,
    InvalidTimeError,
    InvalidIndexError,
}

struct StreamId(u128, u64);

impl ToString for StreamId {
    fn to_string(&self) -> String {
        format!("{}-{}", self.0, self.1)
    }
}

fn parse_stream_id(id: &str, index: u64) -> Result<StreamId, StreamIdError> {
    let (time_ms, index) = if id.is_empty() {
        (0, 0)
    } else if id == "*" {
        (
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Failed to compute millis since unix epoch")
                .as_millis(),
            index,
        )
    } else {
        let id = id.replace('*', index.to_string().as_str());
        match id.split_once('-') {
            Some((time_ms, index)) => match (time_ms.parse::<u128>(), index.parse::<u64>()) {
                (Ok(time_ms), Ok(index)) => (time_ms, index),
                _ => return Err(StreamIdError::ParseError),
            },
            _ => return Err(StreamIdError::ParseError),
        }
    };

    Ok(StreamId(time_ms, index))
}

fn validate_stream_id(id: &str, last_id: &str) -> Result<StreamId, StreamIdError> {
    let StreamId(last_time_ms, last_index) = parse_stream_id(last_id, 0)?;
    let StreamId(time_ms, mut index) = parse_stream_id(id, 0)?;

    if time_ms == 0 && index == 0 {
        return Err(StreamIdError::EqualZeroError);
    }

    if !(last_time_ms == 0 && last_index == 0) {
        if time_ms == last_time_ms && index == 0 {
            index = last_index + 0;
        }

        if time_ms < last_time_ms {
            return Err(StreamIdError::InvalidTimeError);
        }

        if time_ms == last_time_ms && index <= last_index {
            return Err(StreamIdError::InvalidIndexError);
        }
    }

    Ok(StreamId(time_ms, index))
}

#[derive(Debug, Default)]
pub struct Store {
    pub hash_map: HashMap<String, StoreEntry>,
}

impl Store {
    pub fn sweep(&mut self) {
        let mut removable_keys = vec![];

        for (k, v) in self.hash_map.iter() {
            if v.is_expired() {
                removable_keys.push(k.clone());
            }
        }

        for k in removable_keys.drain(..) {
            self.hash_map.remove(&k);
        }
    }

    pub fn get(&self, key: &String) -> Option<&StoreEntryKind> {
        self.hash_map.get(key).and_then(|entry| {
            if !entry.is_expired() {
                Some(entry.value())
            } else {
                None
            }
        })
    }

    pub fn get_mut(&mut self, key: &String) -> Option<&mut StoreEntryKind> {
        self.hash_map.get_mut(key).and_then(|entry| {
            if !entry.is_expired() {
                Some(entry.value_mut())
            } else {
                None
            }
        })
    }

    pub fn remove(&mut self, key: &String) {
        self.hash_map.remove(key);
    }

    pub fn insert_string(&mut self, key: String, value: String, expiry: Duration) {
        let entry = StoreEntry::new(StoreEntryKind::String(value), expiry);
        self.hash_map.insert(key, entry);
    }

    pub fn get_string(&self, key: &String) -> Option<&String> {
        self.get(key).and_then(|kind| match kind {
            StoreEntryKind::String(val) => Some(val),
            _ => None,
        })
    }

    pub fn create_list_mut(&mut self, key: &String) -> &mut Vec<String> {
        let entry = StoreEntry::new(StoreEntryKind::Vector(vec![]), Duration::MAX);
        self.hash_map.insert(key.clone(), entry);
        match self.get_list_mut(key) {
            Some(list) => list,
            _ => unreachable!(),
        }
    }

    pub fn get_or_create_list_mut(&mut self, key: &String) -> &mut Vec<String> {
        if self.get_list(&key).is_none() {
            self.create_list_mut(key)
        } else {
            match self.get_list_mut(&key) {
                Some(list) => list,
                None => unreachable!(),
            }
        }
    }

    pub fn get_list(&self, key: &String) -> Option<&Vec<String>> {
        self.get(key).and_then(|kind| match kind {
            StoreEntryKind::Vector(list) => Some(list),
            _ => None,
        })
    }

    pub fn get_list_mut(&mut self, key: &String) -> Option<&mut Vec<String>> {
        self.get_mut(key).and_then(|kind| match kind {
            StoreEntryKind::Vector(list) => Some(list),
            _ => None,
        })
    }

    pub fn create_stream_mut(&mut self, key: &String) -> &mut BTreeMap<String, StoreEntryKind> {
        let entry = StoreEntry::new(StoreEntryKind::BTreeMap(BTreeMap::new()), Duration::MAX);
        self.hash_map.insert(key.clone(), entry);
        match self.get_stream_mut(key) {
            Some(map) => map,
            _ => unreachable!(),
        }
    }

    pub fn get_stream_mut(
        &mut self,
        key: &String,
    ) -> Option<&mut BTreeMap<String, StoreEntryKind>> {
        self.get_mut(key).and_then(|kind| match kind {
            StoreEntryKind::BTreeMap(stream) => Some(stream),
            _ => None,
        })
    }

    pub fn create_stream_entry_mut(
        &mut self,
        key: &String,
        id: &String,
    ) -> Result<(&mut HashMap<String, StoreEntryKind>, String), StreamIdError> {
        let stream = match self.get_stream_mut(key) {
            Some(map) => map,
            None => self.create_stream_mut(key),
        };

        let last_id = match stream.last_key_value() {
            Some((key, _)) => key.as_str(),
            None => "",
        };

        let id = validate_stream_id(id, last_id)?;
        let id_str = id.to_string();

        stream.insert(id_str.clone(), StoreEntryKind::HashMap(HashMap::new()));
        match self.get_stream_entry_mut(key, &id_str) {
            Some(map) => Ok((map, id_str)),
            _ => unreachable!(),
        }
    }

    pub fn get_stream_entry_mut(
        &mut self,
        key: &String,
        id: &String,
    ) -> Option<&mut HashMap<String, StoreEntryKind>> {
        let stream = self.get_stream_mut(key)?;
        stream.get_mut(id).and_then(|entry| match entry {
            StoreEntryKind::HashMap(map) => Some(map),
            _ => None,
        })
    }
}

#[derive(Debug)]
pub struct StoreEntry {
    value: StoreEntryKind,
    insertion_time: SystemTime,
    expiry_dur: Option<Duration>,
}

impl StoreEntry {
    pub fn new(value: StoreEntryKind, expiry: Duration) -> Self {
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

    pub fn set_value(&mut self, value: StoreEntryKind) {
        self.value = value;
    }

    pub fn value(&self) -> &StoreEntryKind {
        &self.value
    }

    pub fn value_mut(&mut self) -> &mut StoreEntryKind {
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

#[derive(Debug)]
pub enum StoreEntryKind {
    String(String),
    Integer(i64),
    Double(f64),
    Set(HashSet<String>),
    Vector(Vec<String>),
    HashMap(HashMap<String, StoreEntryKind>),
    BTreeMap(BTreeMap<String, StoreEntryKind>),
}

impl ToRespValue for StoreEntryKind {
    fn to_resp_value(&self) -> RespKind {
        match self {
            Self::String(strg) => strg.to_resp_value(),
            Self::Integer(int) => int.to_resp_value(),
            Self::Double(double) => double.to_resp_value(),
            Self::Set(set) => set.to_resp_value(),
            Self::Vector(vec) => vec.to_resp_value(),
            Self::HashMap(hash_map) => hash_map.to_resp_value(),
            Self::BTreeMap(btree_map) => btree_map.to_resp_value(),
        }
    }
}
