mod resp;

use std::{
    io::{BufRead, BufReader, BufWriter, Write},
    net::TcpListener,
    thread,
};

use resp::{RespKind, RespParser, ToRespValue};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    println!("Listening on 127.0.0.1:6379");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(move || {
                    let mut resp_parser =
                        RespParser::new(BufReader::new(&stream), BufWriter::new(&stream));

                    loop {
                        match resp_parser.decode() {
                            Ok(RespKind::SimpleString(val)) => {
                                if val == "PING" {
                                    resp_parser
                                        .encode(RespKind::SimpleString("PONG".to_string()))
                                        .unwrap();
                                }
                            }
                            Ok(RespKind::Array(mut data)) => {
                                let cmd = match data.remove(0) {
                                    RespKind::SimpleString(val) => val.to_lowercase(),
                                    RespKind::BulkString(val) => val.to_lowercase(),
                                    _ => continue,
                                };

                                match cmd.as_str() {
                                    "ping" => {
                                        resp_parser
                                            .encode(RespKind::SimpleString("PONG".to_string()))
                                            .unwrap();
                                    }
                                    "echo" => match data.get(0) {
                                        Some(RespKind::BulkString(val)) => {
                                            resp_parser
                                                .encode(RespKind::BulkString(val.clone()))
                                                .unwrap();
                                        }
                                        _ => continue,
                                    },
                                    _ => todo!(),
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
