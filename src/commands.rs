use std::{
    collections::VecDeque,
    ops::{
        AddAssign,
        Bound::{Excluded, Included},
    },
    sync::Arc,
    time::Duration,
};

use parking_lot::RwLock;

use crate::{
    arguments::{ArgumentError, ArgumentParser},
    condvar::{CondvarRead, CondvarWrite},
    resp::{RespError, RespKind, RespParser, ToRespValue},
    server::{ReplicationData, ReplicationRole},
    store::{Store, StoreEntryKind, StreamIdError},
};

type CommandResult = Result<RespKind, CommandError>;

#[derive(Debug)]
pub enum CommandError {
    ParseError(RespError),
    ArgumentError(ArgumentError),
    InvalidCommand(String),
}

impl From<RespError> for CommandError {
    fn from(value: RespError) -> Self {
        CommandError::ParseError(value)
    }
}

impl From<ArgumentError> for CommandError {
    fn from(value: ArgumentError) -> Self {
        CommandError::ArgumentError(value)
    }
}

#[derive(Default)]
pub struct CommandContextInner {
    pub store: RwLock<Store>,
    pub repl_data: RwLock<ReplicationData>,
    pub list_cv: CondvarWrite,
    pub stream_cv: CondvarRead,
}

#[derive(Default, Clone)]
pub struct SharedCommandContext {
    pub inner: Arc<CommandContextInner>,
}

pub struct CommandHandler<'a> {
    rp: RespParser<'a>,
    ctx: SharedCommandContext,
    cmd_queue: VecDeque<(String, ArgumentParser)>,
    multi_mode: bool,
}

impl<'a> CommandHandler<'a> {
    pub fn new(rp: RespParser<'a>, ctx: SharedCommandContext) -> Self {
        Self {
            rp,
            ctx,
            cmd_queue: VecDeque::new(),
            multi_mode: false,
        }
    }

    fn parse_cmd(&self, data: RespKind) -> Option<(String, ArgumentParser)> {
        match data {
            RespKind::SimpleString(val) | RespKind::BulkString(val) => {
                Some((val, ArgumentParser::new()))
            }
            RespKind::Array(mut args) => {
                let cmd = match args.remove(0) {
                    RespKind::SimpleString(val) => val.to_lowercase(),
                    RespKind::BulkString(val) => val.to_lowercase(),
                    _ => return None,
                };
                let args = ArgumentParser::from(args);

                Some((cmd, args))
            }
            _ => None,
        }
    }

    fn exec_cmd(&mut self, cmd: String, args: ArgumentParser) -> CommandResult {
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
            "incr" => self.incr(args),
            "multi" => self.multi(),
            "exec" => self.exec(),
            "discard" => self.discard(),
            "info" => self.info(args),
            "replconf" => self.replconf(args),
            "psync" => self.psync(args),
            _ => Err(CommandError::InvalidCommand(cmd)),
        }
    }

    pub fn parse(&mut self) -> Result<(), CommandError> {
        let data = self.rp.decode()?;

        let (cmd, args) = match self.parse_cmd(data) {
            Some((cmd, args)) => (cmd, args),
            None => return Ok(()),
        };

        if self.multi_mode && cmd != "exec" && cmd != "discard" {
            self.cmd_queue.push_back((cmd, args));
            return Ok(self.rp.encode(&resp_sstr!("QUEUED"))?);
        }

        let res = self.exec_cmd(cmd, args)?;
        Ok(self.rp.encode(&res)?)
    }

    fn ping(&mut self) -> CommandResult {
        Ok(resp_sstr!("PONG"))
    }

    fn echo(&mut self, mut args: ArgumentParser) -> CommandResult {
        let arg = args.consume_string()?;
        Ok(arg.to_resp_value())
    }

    fn typec(&mut self, mut args: ArgumentParser) -> CommandResult {
        let key = args.consume_string()?;

        let store = self.ctx.inner.store.read();
        let value_type = match store.get(&key) {
            Some(StoreEntryKind::String(_)) => "string",
            Some(StoreEntryKind::Vector(_)) => "list",
            Some(StoreEntryKind::BTreeMap(_)) => "stream",
            _ => "none",
        };

        Ok(resp_sstr!(value_type))
    }

    fn get(&mut self, mut args: ArgumentParser) -> CommandResult {
        let key = args.consume_string()?;

        let store = self.ctx.inner.store.read();
        Ok(match store.get(&key) {
            Some(val) => val.to_resp_value(),
            None => resp_nbstr!(),
        })
    }

    fn set(&mut self, mut args: ArgumentParser) -> CommandResult {
        let key = args.consume_string()?;
        let value = args.consume_string()?;

        let mut expiry = Duration::MAX;

        if let Some((key, value)) = args.try_consume_param_from_type() {
            expiry = match key.as_str() {
                "PX" => Duration::from_millis(value),
                "EX" => Duration::from_secs(value),
                _ => expiry,
            }
        }

        let mut store = self.ctx.inner.store.write();
        store.insert(
            key.clone(),
            match value.parse::<i64>() {
                Ok(val) => StoreEntryKind::Integer(val),
                Err(_) => StoreEntryKind::String(value.clone()),
            },
            expiry,
        );
        Ok(resp_sstr!("OK"))
    }

    fn llen(&mut self, mut args: ArgumentParser) -> CommandResult {
        let list_name = args.consume_string()?;

        let store = self.ctx.inner.store.read();
        let len = store.get_list(&list_name).map_or(0, |l| l.len()) as i64;
        Ok(resp_int!(len))
    }

    fn lrange(&mut self, mut args: ArgumentParser) -> CommandResult {
        let list_name = args.consume_string()?;
        let mut start_i = args.consume_int()?;
        let mut end_i = args.consume_int()?;

        let store = self.ctx.inner.store.read();
        let list = match store.get_list(&list_name) {
            Some(list) => list,
            None => return Ok(resp_arr!(vec![])),
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
        Ok(new_list.to_resp_value())
    }

    fn lpush(&mut self, mut args: ArgumentParser) -> CommandResult {
        let list_name = args.consume_string()?;

        let mut store = self.ctx.inner.store.write();
        let list = store.get_or_create_list_mut(&list_name);

        args.consume_all_strings()
            .into_iter()
            .for_each(|arg| list.insert(0, arg));

        self.ctx.inner.list_cv.notify_one();
        Ok(resp_int!(list.len() as i64))
    }

    fn blpop(&mut self, mut args: ArgumentParser) -> CommandResult {
        let list_name = args.consume_string()?;
        let mut timeout = args.consume_duration_secs().unwrap_or(Duration::ZERO);
        if timeout == Duration::ZERO {
            timeout = Duration::MAX
        };

        let mut store = self.ctx.inner.store.write();
        loop {
            return match store.get_list_mut(&list_name) {
                Some(list) if !list.is_empty() => Ok(resp_arr!(vec![
                    list_name.to_resp_value(),
                    list.remove(0).to_resp_value()
                ])),
                _ => {
                    let res = self.ctx.inner.list_cv.wait_for(&mut store, timeout);

                    if res.timed_out() {
                        return Ok(resp_narr!());
                    }

                    continue;
                }
            };
        }
    }

    fn lpop(&mut self, mut args: ArgumentParser) -> CommandResult {
        let list_name = args.consume_string()?;
        let pop_count = args.consume_int().unwrap_or(1) as usize;

        let mut store = self.ctx.inner.store.write();
        match store.get_list_mut(&list_name) {
            Some(list) if !list.is_empty() => {
                let removed: Vec<_> = list.drain(..pop_count.min(list.len())).collect();
                if removed.len() > 1 {
                    Ok(removed.to_resp_value())
                } else {
                    Ok(removed[0].to_resp_value())
                }
            }
            _ => Ok(resp_nbstr!()),
        }
    }

    fn rpush(&mut self, mut args: ArgumentParser) -> CommandResult {
        let list_name = args.consume_string()?;

        let mut store = self.ctx.inner.store.write();
        let list = store.get_or_create_list_mut(&list_name);

        args.consume_all_strings()
            .into_iter()
            .for_each(|arg| list.push(arg));

        self.ctx.inner.list_cv.notify_one();
        Ok(resp_int!(list.len() as i64))
    }

    fn xadd(&mut self, mut args: ArgumentParser) -> CommandResult {
        let key = args.consume_string()?;
        let id = args.consume_string()?;

        let mut kv_pairs = vec![];
        while !args.is_empty() {
            let key = args.consume_string()?;
            let value = args.consume_string()?;
            kv_pairs.push((key.clone(), value.clone()));
        }

        let mut store = self.ctx.inner.store.write();
        match store.create_stream_entry_mut(&key, &id) {
            Ok((entry, id)) => {
                for (k, v) in kv_pairs.drain(..) {
                    entry.insert(k, StoreEntryKind::String(v));
                }
                self.ctx.inner.stream_cv.notify_one();
                Ok(id.to_resp_value())
            }
            Err(StreamIdError::ParseError) => {
                Ok(resp_serr!("ERR The ID specified in XADD is malformed"))
            }
            Err(StreamIdError::InvalidTimeError | StreamIdError::InvalidIndexError) => {
                Ok(resp_serr!(
                    "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                ))
            }
            Err(StreamIdError::EqualZeroError) => Ok(resp_serr!(
                "ERR The ID specified in XADD must be greater than 0-0"
            )),
        }
    }

    fn xrange(&mut self, mut args: ArgumentParser) -> CommandResult {
        let key = args.consume_string()?;
        let start_id = args.consume_string()?;
        let mut end_id = args.consume_string()?;

        if end_id == "+" {
            end_id = String::from("?");
        }

        let store = self.ctx.inner.store.read();
        match store.search_stream_entries(
            &key,
            (Included(start_id.as_str()), Included(end_id.as_str())),
        ) {
            Some(results) => Ok(results.to_resp_value()),
            None => Ok(resp_narr!()),
        }
    }

    fn xread(&mut self, mut args: ArgumentParser) -> CommandResult {
        let mode = args.consume_string()?.to_lowercase();

        if mode == "block" {
            let mut timeout = args.consume_duration_ms().unwrap_or(Duration::ZERO);
            if timeout == Duration::ZERO {
                timeout = Duration::MAX
            };

            let _ = args.consume_string()?; // Remove `stream`
            self.xread_block(args, timeout)
        } else {
            self.xread_nonblock(args)
        }
    }

    fn xread_nonblock(&mut self, args: ArgumentParser) -> CommandResult {
        if args.len() % 2 != 0 {
            return Err(ArgumentError::InvalidArgCount.into());
        }

        let half = args.len() / 2;
        let mut keys = args;
        let mut ids = keys.split_off(half);

        let mut key_id_pairs = Vec::with_capacity(half);
        for _ in 0..half {
            let key = keys.consume_string()?;
            let id = ids.consume_string()?;
            key_id_pairs.push((key, id));
        }

        let store = self.ctx.inner.store.read();

        let mut payload = vec![];
        for (key, id) in key_id_pairs {
            let results =
                match store.search_stream_entries(&key, (Included(id.as_str()), Included("?"))) {
                    Some(results) => results,
                    None => StoreEntryKind::Vector(vec![]),
                };
            payload.push(vec![StoreEntryKind::String(key), results]);
        }

        Ok(payload.to_resp_value())
    }

    fn xread_block(&mut self, mut args: ArgumentParser, timeout: Duration) -> CommandResult {
        let key = args.consume_string()?;
        let mut id = args.consume_string()?;

        let mut store = self.ctx.inner.store.read();

        if id == "$" {
            id = store
                .get_stream(&key)
                .and_then(|s| s.last_key_value().map(|(k, _)| k.clone()))
                .unwrap_or(id);
        }

        loop {
            return match store.search_stream_entries(&key, (Excluded(id.as_str()), Included("?"))) {
                Some(results) => {
                    let payload = vec![vec![StoreEntryKind::String(key.to_string()), results]];
                    Ok(payload.to_resp_value())
                }
                None => {
                    let res = self.ctx.inner.stream_cv.wait_for(&mut store, timeout);

                    if res.timed_out() {
                        return Ok(resp_narr!());
                    }

                    continue;
                }
            };
        }
    }

    fn incr(&mut self, mut args: ArgumentParser) -> CommandResult {
        let key = args.consume_string()?;

        let mut store = self.ctx.inner.store.write();
        match store.get_mut(&key) {
            Some(StoreEntryKind::Integer(val)) => {
                val.add_assign(1);
                Ok(resp_int!(*val))
            }
            Some(_) => Ok(resp_serr!("ERR value is not an integer or out of range")),
            None => {
                store.insert_integer(key.to_string(), 1);
                Ok(resp_int!(1))
            }
        }
    }

    fn multi(&mut self) -> CommandResult {
        self.multi_mode = true;
        Ok(resp_sstr!("OK"))
    }

    fn exec(&mut self) -> CommandResult {
        if !self.multi_mode {
            return Ok(resp_serr!("ERR EXEC without MULTI"));
        }

        self.multi_mode = false;

        let cmds: Vec<_> = self.cmd_queue.drain(..).collect();
        let mut outputs = vec![];
        for (cmd, args) in cmds {
            outputs.push(self.exec_cmd(cmd, args)?);
        }
        Ok(resp_arr!(outputs))
    }

    fn discard(&mut self) -> CommandResult {
        if !self.multi_mode {
            return Ok(resp_serr!("ERR DISCARD without MULTI"));
        }

        self.multi_mode = false;
        self.cmd_queue.clear();
        Ok(resp_sstr!("OK"))
    }

    fn info(&self, mut args: ArgumentParser) -> CommandResult {
        let option = args.consume_string()?;

        let repl_data = self.ctx.inner.repl_data.read();

        let role = match repl_data.role {
            ReplicationRole::Master => "master",
            ReplicationRole::Slave(_) => "slave",
        };

        if option == "replication" {
            Ok(resp_bstr!(format!(
                "role:{}\nmaster_replid:{}\nmaster_repl_offset:{}",
                role, repl_data.id, repl_data.offset
            )))
        } else {
            Ok(resp_nbstr!())
        }
    }

    fn replconf(&self, _: ArgumentParser) -> CommandResult {
        Ok(resp_sstr!("OK"))
    }

    fn psync(&mut self, _: ArgumentParser) -> CommandResult {
        let repl_data = self.ctx.inner.repl_data.read();
        let empty_rdb_file = vec![
            0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09, 0x72, 0x65, 0x64,
            0x69, 0x73, 0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, 0xfa, 0x0a,
            0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x62, 0x69, 0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05,
            0x63, 0x74, 0x69, 0x6d, 0x65, 0xc2, 0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08, 0x75, 0x73,
            0x65, 0x64, 0x2d, 0x6d, 0x65, 0x6d, 0xc2, 0xb0, 0xc4, 0x10, 0x00, 0xfa, 0x08, 0x61,
            0x6f, 0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, 0xc0, 0x00, 0xff, 0xf0, 0x6e, 0x3b, 0xfe,
            0xc0, 0xff, 0x5a, 0xa2,
        ];
        self.rp
            .encode(&resp_sstr!(format!("FULLRESYNC {} 0", repl_data.id)))?;
        Ok(RespKind::RdbFile(empty_rdb_file))
    }
}
