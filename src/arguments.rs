use std::{collections::VecDeque, str::FromStr, time::Duration};

use crate::resp::RespKind;

#[derive(Debug)]
pub enum ArgumentError {
    InvalidType(&'static str),
    InvalidArgCount,
}

#[derive(Debug)]
pub struct ArgumentParser {
    args: VecDeque<RespKind>,
}

impl ArgumentParser {
    pub fn new() -> Self {
        Self {
            args: VecDeque::new(),
        }
    }

    pub fn from(args: impl Into<VecDeque<RespKind>>) -> Self {
        Self { args: args.into() }
    }

    pub fn is_empty(&self) -> bool {
        self.args.is_empty()
    }

    pub fn len(&self) -> usize {
        self.args.len()
    }

    pub fn split_off(&mut self, mid: usize) -> ArgumentParser {
        let new = self.args.split_off(mid);
        ArgumentParser::from(new)
    }

    pub fn try_consume_from_type<T: FromStr>(&mut self) -> Option<T> {
        match self.args.front() {
            Some(RespKind::BulkString(s)) => {
                if let Ok(v) = s.parse() {
                    let _ = self.args.pop_front();
                    return Some(v);
                }
                None
            }
            _ => None,
        }
    }

    pub fn try_consume_param_from_type<T: FromStr>(&mut self) -> Option<(String, T)> {
        match (self.args.get(0), self.args.get(1)) {
            (Some(RespKind::BulkString(k)), Some(RespKind::BulkString(v))) => {
                let k = k.clone();
                if let Ok(v) = v.parse::<T>() {
                    let _ = self.args.pop_front();
                    let _ = self.args.pop_front();
                    Some((k, v))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn consume_string(&mut self) -> Result<String, ArgumentError> {
        match self.args.pop_front() {
            Some(RespKind::BulkString(s)) => Ok(s),
            _ => Err(ArgumentError::InvalidType("string")),
        }
    }

    pub fn consume_all_strings(&mut self) -> Vec<String> {
        self.args
            .clone()
            .into_iter()
            .filter_map(|x| match x {
                RespKind::BulkString(val) => Some(val),
                _ => None,
            })
            .collect()
    }

    pub fn consume_int(&mut self) -> Result<i64, ArgumentError> {
        match self.try_consume_from_type() {
            Some(i) => Ok(i),
            None => Err(ArgumentError::InvalidType("integer")),
        }
    }

    pub fn consume_duration_secs(&mut self) -> Result<Duration, ArgumentError> {
        match self.try_consume_from_type() {
            Some(f) => Ok(Duration::from_secs_f64(f)),
            None => Err(ArgumentError::InvalidType("duration")),
        }
    }

    pub fn consume_duration_ms(&mut self) -> Result<Duration, ArgumentError> {
        match self.try_consume_from_type() {
            Some(i) => Ok(Duration::from_millis(i)),
            None => Err(ArgumentError::InvalidType("duration")),
        }
    }
}
