#[macro_use]
mod resp;
mod store;

use std::{
    collections::HashMap,
    io::{BufReader, BufWriter},
    net::TcpListener,
    sync::{Arc, RwLock},
    thread,
    time::Duration,
};

use resp::{RespKind, RespParser, ToRespValue};
use store::StoreEntry;

const STORE_SWEEP_WAIT: Duration = Duration::from_secs(30);

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    println!("Listening on 127.0.0.1:6379");

    let kv_store = Arc::new(RwLock::new(HashMap::<String, StoreEntry>::new()));
    let arr_store = Arc::new(RwLock::new(HashMap::<String, Vec<RespKind>>::new()));

    let sweeper_kv_store = Arc::clone(&kv_store);

    // Periodically sweep store and remove expired entries
    thread::spawn(move || {
        loop {
            thread::sleep(STORE_SWEEP_WAIT);

            // Introduce scope to drop write lock after the sweep is finished
            {
                let mut kv_store_handle = sweeper_kv_store.write().unwrap();
                let mut removable_keys = vec![];

                for (k, v) in kv_store_handle.iter() {
                    if v.is_expired() {
                        removable_keys.push(k.clone());
                    }
                }

                for k in removable_keys.iter() {
                    kv_store_handle.remove(k);
                }
            }
        }
    });

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let kv_store = Arc::clone(&kv_store);
                let arr_store = Arc::clone(&arr_store);

                thread::spawn(move || {
                    let mut resp_parser =
                        RespParser::new(BufReader::new(&stream), BufWriter::new(&stream));

                    loop {
                        match resp_parser.decode() {
                            Ok(RespKind::SimpleString(val)) => match val.to_lowercase().as_str() {
                                "ping" => {
                                    resp_parser.encode(&resp_sstr!("PONG")).unwrap();
                                }
                                _ => continue,
                            },
                            Ok(RespKind::Array(mut data)) => {
                                let cmd = match data.remove(0) {
                                    RespKind::SimpleString(val) => val.to_lowercase(),
                                    RespKind::BulkString(val) => val.to_lowercase(),
                                    _ => continue,
                                };
                                let mut args = data;

                                match cmd.as_str() {
                                    "ping" => {
                                        resp_parser.encode(&resp_sstr!("PONG")).unwrap();
                                    }
                                    "echo" => {
                                        if let Some(RespKind::BulkString(val)) = args.get(0) {
                                            resp_parser.encode(&val.to_resp_value()).unwrap();
                                        }
                                    }
                                    "get" => {
                                        if let Some(key) = args.get(0) {
                                            let encoded_key = &key.encode();

                                            if let Some(entry) =
                                                kv_store.read().unwrap().get(encoded_key)
                                                && !entry.is_expired()
                                            {
                                                resp_parser.encode(entry.value()).unwrap();
                                            } else {
                                                resp_parser.encode(&resp_nbstr!()).unwrap();
                                            }
                                        }
                                    }
                                    "set" => {
                                        if let (Some(key), Some(val)) = (args.get(0), args.get(1)) {
                                            let mut expiry = Duration::MAX;

                                            match (args.get(2), args.get(3)) {
                                                (
                                                    Some(RespKind::BulkString(arg)),
                                                    Some(RespKind::BulkString(expiry_ms)),
                                                ) if arg == "PX" => {
                                                    expiry = Duration::from_millis(
                                                        expiry_ms.parse().unwrap(),
                                                    );
                                                }
                                                _ => {}
                                            }

                                            kv_store.write().unwrap().insert(
                                                key.encode(),
                                                StoreEntry::new(val.clone(), expiry),
                                            );
                                            resp_parser.encode(&resp_sstr!("OK")).unwrap();
                                        }
                                    }
                                    "rpush" => {
                                        let list_name = match args.remove(0) {
                                            RespKind::BulkString(val) => val.clone(),
                                            _ => continue,
                                        };

                                        let mut arr_store_handle = arr_store.write().unwrap();
                                        let arr = arr_store_handle
                                            .entry(list_name)
                                            .and_modify(|arr| {
                                                args.iter()
                                                    .for_each(|entry| arr.push(entry.clone()))
                                            })
                                            .or_insert(args.clone());

                                        resp_parser.encode(&resp_int!(arr.len() as i64)).unwrap();
                                    }
                                    "lrange" => {
                                        let list_name = match args.get(0) {
                                            Some(RespKind::BulkString(val)) => val,
                                            _ => continue,
                                        };

                                        let start_index: usize = match args.get(1) {
                                            Some(RespKind::BulkString(val)) => val.parse().unwrap(),
                                            _ => continue,
                                        };

                                        let end_index: usize = match args.get(2) {
                                            Some(RespKind::BulkString(val)) => val.parse().unwrap(),
                                            _ => continue,
                                        };

                                        let arr_store_handle = arr_store.read().unwrap();
                                        let arr = arr_store_handle
                                            .get(list_name)
                                            .cloned()
                                            .unwrap_or_default();

                                        resp_parser
                                            .encode(&resp_arr!(
                                                arr.get(start_index..end_index)
                                                    .map(|x| x.to_vec())
                                                    .unwrap_or_default()
                                            ))
                                            .unwrap();
                                    }
                                    _ => {
                                        resp_parser
                                            .encode(&resp_berr!("Command not implemented"))
                                            .unwrap();
                                    }
                                }
                            }
                            _ => continue,
                        }
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
