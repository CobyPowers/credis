use std::{
    io::{Read, Write},
    ops::Bound::Included,
    sync::Arc,
    time::Duration,
};

use parking_lot::{Condvar, Mutex, RwLock, RwLockWriteGuard, WaitTimeoutResult};

use crate::{
    resp::{RespError, RespKind, RespParser, ToRespValue},
    store::{Store, StoreEntryKind, StreamIdError},
};

type CommandArguments = Vec<RespKind>;
type CommandReturn = Result<(), RespError>;

// We need a middleman structure that allows us to use Condvars
// with RwLocks since parking_lot Condvars don't support anything other
// than MutexGuards. There is probably a better way of doing this,
// but I'm still learning rust :')
//
// https://github.com/Amanieu/parking_lot/issues/165
#[derive(Default)]
pub struct CondvarAny {
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

    // fn notify_all(&self) {
    //     let _guard = self.mtx.lock();
    //     self.cv.notify_all();
    // }
}

#[derive(Default)]
pub struct CommandContextInner {
    pub store: RwLock<Store>,
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
            "xrange" => self.xrange(args),
            "xread" => self.xread(args),
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
            Some(RespKind::BulkString(val)) => val,
            _ => return Ok(()),
        };

        let store = self.ctx.inner.store.read();
        let value_type = match store.get(key) {
            Some(StoreEntryKind::String(_)) => "string",
            Some(StoreEntryKind::Vector(_)) => "list",
            Some(StoreEntryKind::BTreeMap(_)) => "stream",
            _ => "none",
        };

        self.rp.encode(&resp_sstr!(value_type))
    }

    fn get(&mut self, args: CommandArguments) -> CommandReturn {
        let key = match args.get(0) {
            Some(RespKind::BulkString(val)) => val,
            _ => return Ok(()),
        };

        let store = self.ctx.inner.store.read();
        match store.get_string(&key) {
            Some(val) => self.rp.encode(&val.to_resp_value()),
            None => self.rp.encode(&resp_nbstr!()),
        }
    }

    fn set(&mut self, args: CommandArguments) -> CommandReturn {
        let (key, value) = match (args.get(0), args.get(1)) {
            (Some(RespKind::BulkString(key)), Some(RespKind::BulkString(val))) => (key, val),
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

        let mut store = self.ctx.inner.store.write();
        store.insert_string(key.clone(), value.clone(), expiry);
        self.rp.encode(&resp_sstr!("OK"))
    }

    fn llen(&mut self, args: CommandArguments) -> CommandReturn {
        let list_name = match args.get(0) {
            Some(RespKind::BulkString(val)) => val,
            _ => return Ok(()),
        };

        let store = self.ctx.inner.store.read();
        let len = store.get_list(list_name).map_or(0, |l| l.len()) as i64;
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

        let store = self.ctx.inner.store.read();
        let list = match store.get_list(list_name) {
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
        self.rp.encode(&new_list.to_resp_value())
    }

    fn lpush(&mut self, args: CommandArguments) -> CommandReturn {
        let list_name = match args.get(0) {
            Some(RespKind::BulkString(val)) => val,
            _ => return Ok(()),
        };

        let args: Vec<_> = args[1..]
            .iter()
            .filter_map(|x| match x {
                RespKind::BulkString(val) => Some(val),
                _ => None,
            })
            .collect();

        let mut store = self.ctx.inner.store.write();
        let list = store.get_or_create_list_mut(list_name);

        for arg in args {
            list.insert(0, arg.clone());
        }

        self.ctx.inner.list_cv.notify_one();
        self.rp.encode(&resp_int!(list.len() as i64))
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

        let mut store = self.ctx.inner.store.write();
        loop {
            return match store.get_list_mut(list_name) {
                Some(list) if !list.is_empty() => self.rp.encode(&resp_arr!(vec![
                    list_name.to_resp_value(),
                    list.remove(0).to_resp_value()
                ])),
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

        let mut store = self.ctx.inner.store.write();
        match store.get_list_mut(list_name) {
            Some(list) if !list.is_empty() => {
                let removed: Vec<_> = list.drain(..pop_count.min(list.len())).collect();
                if removed.len() > 1 {
                    self.rp.encode(&removed.to_resp_value())
                } else {
                    self.rp.encode(&removed[0].to_resp_value())
                }
            }
            _ => self.rp.encode(&resp_nbstr!()),
        }
    }

    fn rpush(&mut self, args: CommandArguments) -> CommandReturn {
        let list_name = match args.get(0) {
            Some(RespKind::BulkString(val)) => val,
            _ => return Ok(()),
        };

        let args: Vec<_> = args[1..]
            .iter()
            .filter_map(|x| match x {
                RespKind::BulkString(val) => Some(val),
                _ => None,
            })
            .collect();

        let mut store = self.ctx.inner.store.write();
        let list = store.get_or_create_list_mut(list_name);

        for arg in args {
            list.push(arg.clone());
        }

        self.ctx.inner.list_cv.notify_one();
        self.rp.encode(&resp_int!(list.len() as i64))
    }

    fn xadd(&mut self, args: CommandArguments) -> CommandReturn {
        let key = match args.get(0) {
            Some(RespKind::BulkString(val)) => val,
            _ => return Ok(()),
        };

        let id = match args.get(1) {
            Some(RespKind::BulkString(val)) => val,
            _ => return Ok(()),
        };

        let mut args = &args[2..];
        let mut kv_pairs = vec![];
        while !args.is_empty() {
            let key = match args.get(0) {
                Some(RespKind::BulkString(key)) => key,
                _ => return Ok(()),
            };

            let value = match args.get(1) {
                Some(val) => val,
                _ => return Ok(()),
            };

            kv_pairs.push((key.clone(), value.clone()));
            args = &args[2..];
        }

        let mut store = self.ctx.inner.store.write();
        match store.create_stream_entry_mut(key, id) {
            Ok((entry, id)) => {
                for (k, v) in kv_pairs.drain(..) {
                    // TODO: Check if stream entry values can contain types other than strings,
                    // integers, and doubles
                    let entry_v = match v {
                        RespKind::BulkString(val) => StoreEntryKind::String(val),
                        RespKind::Integer(val) => StoreEntryKind::Integer(val),
                        RespKind::Double(val) => StoreEntryKind::Double(val),
                        _ => continue
                    };

                    entry.insert(k, entry_v);
                }
                self.rp.encode(&id.to_resp_value())
            }
            Err(StreamIdError::ParseError) => self.rp.encode(&resp_serr!(
                "ERR The ID specified in XADD is malformed"
            )),
            Err(StreamIdError::InvalidTimeError | StreamIdError::InvalidIndexError) => self.rp.encode(&resp_serr!(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
            )),
            Err(StreamIdError::EqualZeroError) => self.rp.encode(&resp_serr!(
                "ERR The ID specified in XADD must be greater than 0-0"
            )),
        }
    }

    fn xrange(&mut self, args: CommandArguments) -> CommandReturn {
        let key = match args.get(0) {
            Some(RespKind::BulkString(val)) => val,
            _ => return Ok(()),
        };

        let start_id = match args.get(1) {
            Some(RespKind::BulkString(val)) => val.as_str(),
            _ => return Ok(()),
        };

        let mut end_id = match args.get(2) {
            Some(RespKind::BulkString(val)) => val.as_str(),
            _ => return Ok(()),
        };

        // '=' comes after '9' in the ascii table which forces the map to search until the end
        if end_id == "+" {
            end_id = "=";
        }

        let store = self.ctx.inner.store.read();
        let stream = match store.get_stream(key) {
            Some(stream) => stream,
            None => return self.rp.encode(&resp_narr!()),
        };

        let mut entries: Vec<Vec<StoreEntryKind>> = vec![];
        for (_, v) in stream.range::<str, _>((Included(start_id), Included(end_id))) {
            match v {
                StoreEntryKind::HashMap(map) => {
                    entries.push(
                        map.iter()
                            .flat_map(|(k, v)| vec![StoreEntryKind::String(k.clone()), v.clone()])
                            .collect(),
                    );
                }
                _ => continue,
            }
        }

        self.rp.encode(&entries.to_resp_value())
    }

    fn xread(&mut self, args: CommandArguments) -> CommandReturn {
        let key = match args.get(0) {
            Some(RespKind::BulkString(val)) => val,
            _ => return Ok(()),
        };

        let id = match args.get(1) {
            Some(RespKind::BulkString(val)) => val,
            _ => return Ok(()),
        };

        let store = self.ctx.inner.store.read();
        let stream_entry = store.get_stream_entry(key, id);
        match stream_entry {
            Some(map) => {
                let entries: Vec<_> = map
                    .iter()
                    .flat_map(|(k, v)| vec![StoreEntryKind::String(k.clone()), v.clone()])
                    .collect();
                self.rp.encode(&entries.to_resp_value())
            }
            None => self.rp.encode(&resp_narr!()),
        }
    }
}
