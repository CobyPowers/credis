#[macro_use]
mod resp;

use std::{
    collections::HashMap,
    io::{BufReader, BufWriter},
    net::TcpListener,
    sync::{Arc, RwLock},
    thread,
};

use resp::{RespKind, RespParser, ToRespValue};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    println!("Listening on 127.0.0.1:6379");

    let store = Arc::new(RwLock::new(HashMap::<String, RespKind>::new()));

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

                                match cmd.as_str() {
                                    "ping" => {
                                        resp_parser.encode(&resp_sstr!("PONG")).unwrap();
                                    }
                                    "echo" => match data.get(0) {
                                        Some(RespKind::BulkString(val)) => {
                                            resp_parser
                                                .encode(&val.clone().to_resp_value())
                                                .unwrap();
                                        }
                                        _ => continue,
                                    },
                                    "get" => {
                                        if let Some(key) = data.get(0) {
                                            if let Some(val) =
                                                store.read().unwrap().get(&key.encode())
                                            {
                                                resp_parser.encode(val).unwrap();
                                            }
                                        }
                                    }
                                    "set" => {
                                        if let (Some(key), Some(val)) = (data.get(0), data.get(1)) {
                                            store
                                                .write()
                                                .unwrap()
                                                .insert(key.encode(), val.clone());
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
