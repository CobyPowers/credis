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

const STORE_SWEEP_DELAY: Duration = Duration::from_secs(30);

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    println!("Listening on 127.0.0.1:6379");

    let store = Arc::new(RwLock::new(HashMap::<String, StoreEntry>::new()));
    let sweeper_store = Arc::clone(&store);

    // Periodically sweep store and remove expired entries
    thread::spawn(move || {
        loop {
            let mut store_handle = sweeper_store.write().unwrap();
            let mut removable_keys = vec![];

            for (k, v) in store_handle.iter() {
                if v.is_expired() {
                    removable_keys.push(k.clone());
                }
            }

            for k in removable_keys.iter() {
                store_handle.remove(k);
            }

            thread::sleep(STORE_SWEEP_DELAY);
        }
    });

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let store = Arc::clone(&store);

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
                                let args = data;

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
                                                store.read().unwrap().get(encoded_key)
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

                                            store.write().unwrap().insert(
                                                key.encode(),
                                                StoreEntry::new(val.clone(), expiry),
                                            );
                                            resp_parser.encode(&resp_sstr!("OK")).unwrap();
                                        }
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
