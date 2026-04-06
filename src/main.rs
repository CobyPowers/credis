#[macro_use]
mod resp;
mod commands;
mod store;

use std::{
    io::{BufReader, BufWriter},
    net::TcpListener,
    thread,
    time::Duration,
};

use crate::{
    commands::{CommandHandler, SharedCommandContext},
    resp::RespParser,
};

const STORE_SWEEP_INTERVAL: Duration = Duration::from_secs(30);

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    println!("Listening on 127.0.0.1:6379");

    let ctx = SharedCommandContext::default();

    // Periodically sweep store and remove expired entries
    let sweeper_ctx = ctx.clone();
    thread::spawn(move || {
        loop {
            thread::sleep(STORE_SWEEP_INTERVAL);

            let mut kv_store_handle = sweeper_ctx.inner.kv_store.write();
            let mut removable_keys = vec![];

            for (k, v) in kv_store_handle.iter() {
                if v.is_expired() {
                    removable_keys.push(k.clone());
                }
            }

            for k in removable_keys.iter() {
                kv_store_handle.remove(k);
            }

            drop(kv_store_handle);
        }
    });

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let ctx = ctx.clone();
                thread::spawn(move || {
                    let rp = RespParser::new(BufReader::new(&stream), BufWriter::new(&stream));
                    let mut handler = CommandHandler::new(rp, ctx);

                    loop {
                        if let Err(e) = handler.parse() {
                            eprintln!("Command handler encountered an error: {:?}", e);
                        }
                    }
                });
            }
            Err(e) => {
                println!("TCP server encountered an error: {}", e);
            }
        }
    }
}
