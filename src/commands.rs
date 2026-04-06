use std::{
    collections::HashMap,
    io::{Read, Write},
    sync::Arc,
    time::Duration,
};

use parking_lot::{Condvar, Mutex, RwLock, RwLockWriteGuard, WaitTimeoutResult};

use crate::{
    resp::{RespError, RespKind, RespParser},
    store::StoreEntry,
};

type CommandArguments = Vec<RespKind>;
type CommandReturn = Result<(), RespError>;

// https://github.com/Amanieu/parking_lot/issues/165
#[derive(Default)]
struct CondvarAny {
    cv: Condvar,
    mtx: Mutex<()>,
}

impl CondvarAny {
    // fn wait<T>(&self, g: &mut RwLockWriteGuard<'_, T>) {
    //     let guard = self.mtx.lock();
    //     RwLockWriteGuard::unlocked(g, || {
    //         let mut guard = guard;
    //         self.cv.wait(&mut guard);
    //     });
    // }

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
    pub arr_store: RwLock<HashMap<String, Vec<RespKind>>>,
    pub arr_cv: CondvarAny,
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

    pub fn parse(&mut self) -> Result<(), RespError> {
        match self.rp.decode() {
            Ok(RespKind::SimpleString(val)) => match val.to_lowercase().as_str() {
                "ping" => self.ping(),
                _ => Ok(()),
            },
            Ok(RespKind::Array(mut data)) => {
                let cmd = match data.remove(0) {
                    RespKind::SimpleString(val) => val.to_lowercase(),
                    RespKind::BulkString(val) => val.to_lowercase(),
                    _ => return Ok(()),
                };
                let args = data;

                match cmd.as_str() {
                    "ping" => self.ping(),
                    "echo" => self.echo(args),
                    "get" => self.get(args),
                    "set" => self.set(args),
                    "llen" => self.llen(args),
                    "lpush" => self.lpush(args),
                    "lpop" => self.lpop(args),
                    "blpop" => self.blpop(args),
                    "rpush" => self.rpush(args),
                    "lrange" => self.lrange(args),
                    _ => Ok(()),
                }
            }
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

    fn get(&mut self, args: CommandArguments) -> CommandReturn {
        match args.get(0) {
            Some(key) => {
                let encoded_key = &key.encode();

                if let Some(entry) = self.ctx.inner.kv_store.read().get(encoded_key)
                    && !entry.is_expired()
                {
                    self.rp.encode(entry.value())
                } else {
                    self.rp.encode(&resp_nbstr!())
                }
            }
            None => Ok(()),
        }
    }

    fn set(&mut self, args: CommandArguments) -> CommandReturn {
        if let (Some(key), Some(val)) = (args.get(0), args.get(1)) {
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

            self.ctx
                .inner
                .kv_store
                .write()
                .insert(key.encode(), StoreEntry::new(val.clone(), expiry));
            return self.rp.encode(&resp_sstr!("OK"));
        }

        Ok(())
    }

    fn llen(&mut self, args: CommandArguments) -> CommandReturn {
        match args.get(0) {
            Some(RespKind::BulkString(val)) => {
                let arr_store_handle = self.ctx.inner.arr_store.read();
                let arr_len = arr_store_handle.get(val).map_or(0, |arr| arr.len());

                self.rp.encode(&resp_int!(arr_len as i64))
            }
            _ => Ok(()),
        }
    }

    fn lrange(&mut self, args: CommandArguments) -> CommandReturn {
        match (args.get(0), args.get(1), args.get(2)) {
            (
                Some(RespKind::BulkString(list_name)),
                Some(RespKind::BulkString(start_index)),
                Some(RespKind::BulkString(end_index)),
            ) => {
                let arr_store_handle = self.ctx.inner.arr_store.read();
                let arr = arr_store_handle.get(list_name).cloned().unwrap_or_default();

                match (start_index.parse::<i64>(), end_index.parse::<i64>()) {
                    (Ok(mut start_index), Ok(mut end_index)) => {
                        if start_index < 0 {
                            start_index += arr.len() as i64;
                        }

                        if end_index < 0 {
                            end_index += arr.len() as i64;
                        }

                        let start_index = start_index.max(0) as usize;
                        let end_index = end_index.max(0) as usize;

                        self.rp.encode(&resp_arr!(
                            arr.get(start_index..=end_index.min(arr.len() - 1))
                                .map(|x| x.to_vec())
                                .unwrap_or_default()
                        ))
                    }
                    _ => Ok(()),
                }
            }
            _ => Ok(()),
        }
    }

    fn lpush(&mut self, args: CommandArguments) -> CommandReturn {
        match args.get(0) {
            Some(RespKind::BulkString(list_name)) => {
                let mut arr_store_handle = self.ctx.inner.arr_store.write();
                let arr = arr_store_handle
                    .entry(list_name.clone())
                    .and_modify(|arr| args.iter().for_each(|entry| arr.insert(0, entry.clone())))
                    .or_insert(args.into_iter().rev().collect());

                self.ctx.inner.arr_cv.notify_one();
                self.rp.encode(&resp_int!(arr.len() as i64))
            }
            _ => Ok(()),
        }
    }

    fn blpop(&mut self, args: CommandArguments) -> CommandReturn {
        match args.get(0) {
            Some(RespKind::BulkString(list_name)) => {
                let wait_timeout = match args.get(1) {
                    Some(RespKind::BulkString(val)) => val.parse::<u64>().unwrap(),
                    _ => u64::MAX,
                };
                let wait_timeout = Duration::from_secs(wait_timeout);

                let mut arr_store_handle = self.ctx.inner.arr_store.write();
                let mut ret_arr = vec![];
                loop {
                    match arr_store_handle.get_mut(list_name) {
                        Some(arr) if !arr.is_empty() => {
                            ret_arr.push(arr.remove(0));
                            break;
                        }
                        _ => {
                            let res = self
                                .ctx
                                .inner
                                .arr_cv
                                .wait_for(&mut arr_store_handle, wait_timeout);

                            if res.timed_out() {
                                self.rp.encode(&resp_narr!())
                            } else {
                                continue;
                            }
                        }
                    };
                }

                drop(arr_store_handle);
                self.ctx.inner.arr_cv.notify_one();
                println!("POPPING VALUE {:?}", ret_arr);
                self.rp.encode(&resp_arr!(ret_arr))
            }
            _ => Ok(()),
        }
    }

    fn lpop(&mut self, args: CommandArguments) -> CommandReturn {
        match args.get(0) {
            Some(RespKind::BulkString(list_name)) => {
                let pop_count = match args.get(1) {
                    Some(RespKind::BulkString(val)) => val.parse::<usize>().unwrap(),
                    _ => 1,
                };

                let mut arr_store_handle = self.ctx.inner.arr_store.write();
                if let Some(arr) = arr_store_handle.get_mut(list_name)
                    && !arr.is_empty()
                {
                    let removed: Vec<_> = arr.drain(..pop_count.min(arr.len())).collect();
                    if removed.len() > 1 {
                        self.rp.encode(&resp_arr!(removed))
                    } else {
                        self.rp.encode(&removed[0])
                    }
                } else {
                    self.rp.encode(&resp_nbstr!())
                }
            }
            _ => Ok(()),
        }
    }

    fn rpush(&mut self, args: CommandArguments) -> CommandReturn {
        match args.get(0) {
            Some(RespKind::BulkString(list_name)) => {
                let mut arr_store_handle = self.ctx.inner.arr_store.write();
                let arr = arr_store_handle
                    .entry(list_name.clone())
                    .and_modify(|arr| args.iter().for_each(|entry| arr.push(entry.clone())))
                    .or_insert(args.clone());

                self.ctx.inner.arr_cv.notify_one();
                self.rp.encode(&resp_int!(arr.len() as i64))
            }
            _ => Ok(()),
        }
    }
}
