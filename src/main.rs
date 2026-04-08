#[macro_use]
mod resp;
mod commands;
mod condvar;
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

            let mut store = sweeper_ctx.inner.store.write();
            store.sweep();
            drop(store);
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
