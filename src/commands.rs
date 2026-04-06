use std::{
    collections::HashMap,
    io::{Read, Write},
    sync::Arc,
    time::Duration,
};

use parking_lot::{Condvar, Mutex, RwLock, RwLockWriteGuard, WaitTimeoutResult};

use crate::{
    resp::{RespError, RespKind, RespParser, ToRespValue},
    store::StoreEntry,
};

type CommandArguments = Vec<RespKind>;
type CommandReturn = Result<(), RespError>;

// We need a middleman structure that allows us to use Condvars
// with RwLocks since parking_lot doesn't support anything other
// than MutexGuards. There is probably a better way of doing this,
// but I'm still learning rust :')
// https://github.com/Amanieu/parking_lot/issues/165
#[derive(Default)]
struct CondvarAny {
    cv: Condvar,
    mtx: Mutex<()>,
}

impl CondvarAny {
    fn wait_for<T>(&self, g: &mut RwLockWriteGuard<'_, T>, timeout: Duration) -> WaitTimeoutResult {
        let guard = self.mtx.lock();
        RwLockWriteGuard::unlocked(g, || {
            let mut guard = guard;
            self.cv.wait_for(&mut guard, timeout)
        })
    }

    fn notify_one(&self) {
        let _guard = self.mtx.lock();
        self.cv.notify_one();
    }

    fn notify_all(&self) {
        let _guard = self.mtx.lock();
        self.cv.notify_all();
    }
}

#[derive(Default)]
pub struct CommandContextInner {
    pub kv_store: RwLock<HashMap<String, StoreEntry>>,
    pub list_cv: CondvarAny,
}

#[derive(Default)]
pub struct SharedCommandContext {
    pub inner: Arc<CommandContextInner>,
}

impl Clone for SharedCommandContext {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

pub struct CommandHandler<R, W>
where
    R: Read,
    W: Write,
{
    // RESP Parser
    rp: RespParser<R, W>,
    ctx: SharedCommandContext,
}

impl<R, W> CommandHandler<R, W>
where
    R: Read,
    W: Write,
{
    pub fn new(rp: RespParser<R, W>, ctx: SharedCommandContext) -> Self {
        Self { rp, ctx }
    }

    fn parse_cmd(&self, data: RespKind) -> Option<(String, Vec<RespKind>)> {
        match data {
            RespKind::SimpleString(val) | RespKind::BulkString(val) => Some((val, vec![])),
            RespKind::Array(mut list) => {
                let cmd = match list.remove(0) {
                    RespKind::SimpleString(val) => val.to_lowercase(),
                    RespKind::BulkString(val) => val.to_lowercase(),
                    _ => return None,
                };
                let args = list;

                Some((cmd, args))
            }
            _ => None,
        }
    }

    pub fn parse(&mut self) -> Result<(), RespError> {
        let data = self.rp.decode()?;

        let (cmd, args) = match self.parse_cmd(data) {
            Some((cmd, args)) => (cmd, args),
            None => return Ok(()),
        };

        match cmd.as_str() {
            "ping" => self.ping(),
            "echo" => self.echo(args),
            "type" => self.typec(args),
            "get" => self.get(args),
            "set" => self.set(args),
            "llen" => self.llen(args),
            "lpush" => self.lpush(args),
            "lpop" => self.lpop(args),
            "blpop" => self.blpop(args),
            "rpush" => self.rpush(args),
            "lrange" => self.lrange(args),
            "xadd" => self.xadd(args),
            _ => Ok(()),
        }
    }

    fn ping(&mut self) -> CommandReturn {
        self.rp.encode(&resp_sstr!("PONG"))
    }

    fn echo(&mut self, args: CommandArguments) -> CommandReturn {
        match args.get(0) {
            Some(val) => self.rp.encode(&val),
            None => Ok(()),
        }
    }

    fn typec(&mut self, args: CommandArguments) -> CommandReturn {
        let key = match args.get(0) {
            Some(key) => key,
            None => return Ok(()),
        };

        let store = self.ctx.inner.kv_store.read();
        let value_type = store
            .get(&key.encode())
            .and_then(|entry| match entry.value() {
                RespKind::BulkString(_) => Some("string"),
                RespKind::Array(_) => Some("list"),
                RespKind::Map(_) => Some("stream"),
                _ => Some("none"),
            })
            .unwrap_or("none");

        self.rp.encode(&resp_sstr!(value_type))
    }

    fn get(&mut self, args: CommandArguments) -> CommandReturn {
        let key = match args.get(0) {
            Some(key) => key,
            None => return Ok(()),
        };
        let key = &key.encode();

        let store = self.ctx.inner.kv_store.read();
        match store.get(key) {
            Some(entry) if !entry.is_expired() => self.rp.encode(entry.value()),
            _ => self.rp.encode(&resp_nbstr!()),
        }
    }

    fn set(&mut self, args: CommandArguments) -> CommandReturn {
        let (key, value) = match (args.get(0), args.get(1)) {
            (Some(key), Some(value)) => (key, value),
            _ => return Ok(()),
        };

        let mut expiry = Duration::MAX;

        if let (Some(RespKind::BulkString(arg1)), Some(RespKind::BulkString(arg2))) =
            (args.get(2), args.get(3))
        {
            if arg1 == "PX" {
                if let Ok(ms) = arg2.parse::<u64>() {
                    expiry = Duration::from_millis(ms);
                }
            } else if arg1 == "EX" {
                if let Ok(ss) = arg2.parse::<u64>() {
                    expiry = Duration::from_secs(ss);
                }
            }
        }

        let mut store = self.ctx.inner.kv_store.write();
        store.insert(key.encode(), StoreEntry::new(value.clone(), expiry));
        self.rp.encode(&resp_sstr!("OK"))
    }

    fn llen(&mut self, args: CommandArguments) -> CommandReturn {
        let list_name = match args.get(0) {
            Some(RespKind::BulkString(val)) => val,
            _ => return Ok(()),
        };

        let store = self.ctx.inner.kv_store.read();
        let len = store
            .get(list_name)
            .and_then(|entry| match entry.value() {
                RespKind::Array(list) => Some(list.len() as i64),
                _ => None,
            })
            .unwrap_or(0);
        self.rp.encode(&resp_int!(len))
    }

    fn lrange(&mut self, args: CommandArguments) -> CommandReturn {
        let (list_name, start_i, end_i) = match (args.get(0), args.get(1), args.get(2)) {
            (
                Some(RespKind::BulkString(list_name)),
                Some(RespKind::BulkString(start_i)),
                Some(RespKind::BulkString(end_i)),
            ) => (list_name, start_i, end_i),
            _ => return Ok(()),
        };

        let (mut start_i, mut end_i) = match (start_i.parse::<i64>(), end_i.parse::<i64>()) {
            (Ok(start_i), Ok(end_i)) => (start_i, end_i),
            _ => return Ok(()),
        };

        let store = self.ctx.inner.kv_store.read();
        let list = match store.get(list_name).and_then(|entry| match entry.value() {
            RespKind::Array(list) => Some(list),
            _ => None,
        }) {
            Some(list) => list,
            None => return self.rp.encode(&resp_arr!(vec![])),
        };

        let len = list.len() as i64;
        if start_i < 0 {
            start_i += len;
        }
        if end_i < 0 {
            end_i += len;
        }

        let start_i = start_i.max(0) as usize;
        let end_i = end_i.max(0) as usize;

        let new_list = list
            .get(start_i..=end_i.min(list.len() - 1))
            .map(|x| x.to_vec())
            .unwrap_or_default();
        self.rp.encode(&resp_arr!(new_list))
    }

    fn lpush(&mut self, mut args: CommandArguments) -> CommandReturn {
        let list_name = match args.remove(0) {
            RespKind::BulkString(val) => val,
            _ => return Ok(()),
        };

        let mut store = self.ctx.inner.kv_store.write();
        let entry = store
            .entry(list_name)
            .and_modify(|entry| match entry.value_mut() {
                RespKind::Array(list) => {
                    args.iter().for_each(|arg| list.insert(0, arg.clone()));
                }
                _ => {
                    entry.set_value(resp_arr!(args.clone().into_iter().rev().collect()));
                }
            })
            .or_insert_with(|| {
                StoreEntry::new(
                    RespKind::Array(args.into_iter().rev().collect()),
                    Duration::MAX,
                )
            });

        self.ctx.inner.list_cv.notify_one();

        let len = match entry.value() {
            RespKind::Array(list) => list.len() as i64,
            _ => 0,
        };
        self.rp.encode(&resp_int!(len))
    }

    fn blpop(&mut self, args: CommandArguments) -> CommandReturn {
        let list_name = match args.get(0) {
            Some(RespKind::BulkString(val)) => val,
            _ => return Ok(()),
        };

        let wait_timeout = match args.get(1) {
            Some(RespKind::BulkString(val)) => val.parse::<f64>().unwrap_or(0f64),
            _ => 0f64,
        };

        let wait_timeout = if wait_timeout > 0f64 {
            Duration::from_secs_f64(wait_timeout)
        } else {
            Duration::MAX
        };

        let mut store = self.ctx.inner.kv_store.write();
        loop {
            return match store
                .get_mut(list_name)
                .and_then(|entry| match entry.value_mut() {
                    RespKind::Array(list) => Some(list),
                    _ => None,
                }) {
                Some(list) if !list.is_empty() => self
                    .rp
                    .encode(&resp_arr!(vec![list_name.to_resp_value(), list.remove(0)])),
                _ => {
                    let res = self.ctx.inner.list_cv.wait_for(&mut store, wait_timeout);

                    if res.timed_out() {
                        return self.rp.encode(&resp_narr!());
                    }

                    continue;
                }
            };
        }
    }

    fn lpop(&mut self, args: CommandArguments) -> CommandReturn {
        let list_name = match args.get(0) {
            Some(RespKind::BulkString(val)) => val,
            _ => return Ok(()),
        };

        let pop_count = match args.get(1) {
            Some(RespKind::BulkString(val)) => val.parse::<usize>().unwrap_or(1),
            _ => 1,
        };

        let mut store = self.ctx.inner.kv_store.write();
        match store
            .get_mut(list_name)
            .and_then(|entry| match entry.value_mut() {
                RespKind::Array(list) => Some(list),
                _ => None,
            }) {
            Some(list) if !list.is_empty() => {
                let removed: Vec<_> = list.drain(..pop_count.min(list.len())).collect();
                if removed.len() > 1 {
                    self.rp.encode(&resp_arr!(removed))
                } else {
                    self.rp.encode(&removed[0])
                }
            }
            _ => self.rp.encode(&resp_nbstr!()),
        }
    }

    fn rpush(&mut self, mut args: CommandArguments) -> CommandReturn {
        let list_name = match args.remove(0) {
            RespKind::BulkString(val) => val,
            _ => return Ok(()),
        };

        let mut store = self.ctx.inner.kv_store.write();
        let entry = store
            .entry(list_name)
            .and_modify(|entry| match entry.value_mut() {
                RespKind::Array(list) => {
                    args.iter().for_each(|arg| list.push(arg.clone()));
                }
                _ => {
                    entry.set_value(resp_arr!(args.clone()));
                }
            })
            .or_insert_with(|| StoreEntry::new(RespKind::Array(args.clone()), Duration::MAX));

        self.ctx.inner.list_cv.notify_one();

        let len = match entry.value() {
            RespKind::Array(list) => list.len() as i64,
            _ => 0,
        };
        self.rp.encode(&resp_int!(len))
    }

    fn xadd(&mut self, args: CommandArguments) -> CommandReturn {
        let stream_key = match args.get(0) {
            Some(RespKind::BulkString(val)) => val,
            _ => return Ok(()),
        };

        let entry_id = match args.get(1) {
            Some(RespKind::BulkString(val)) => val,
            _ => return Ok(()),
        };

        let mut kv_pairs = vec![];
        let mut args = &args[2..];
        while !args.is_empty() {
            let key = match args.get(0) {
                Some(val) => val,
                _ => return Ok(()),
            };

            let value = match args.get(1) {
                Some(val) => val,
                _ => return Ok(()),
            };

            kv_pairs.push((key.encode(), value.clone()));
            args = &args[2..];
        }

        let mut store = self.ctx.inner.kv_store.write();
        let entry = store.entry(stream_key.clone()).or_insert(StoreEntry::new(
            RespKind::Map(HashMap::new()),
            Duration::MAX,
        ));
        let stream = match entry.value_mut() {
            RespKind::Map(stream) => stream,
            _ => return Ok(()),
        };
        let stream_entry = match stream
            .entry(entry_id.clone())
            .or_insert(RespKind::Map(HashMap::new()))
        {
            RespKind::Map(map) => map,
            _ => return Ok(()),
        };

        for (k, v) in kv_pairs.drain(..) {
            stream_entry.insert(k, v);
        }

        self.rp.encode(&resp_bstr!(entry_id))
    }
}
