#[macro_use]
mod resp;
mod arguments;
mod commands;
mod condvar;
mod server;
mod store;

use std::{thread, time::Duration};

use clap::Parser;

use crate::server::Server;

const STORE_SWEEP_INTERVAL: Duration = Duration::from_secs(45);

#[derive(Debug, Parser)]
struct RedisArgs {
    #[arg(short, long, default_value_t = 6379)]
    port: u16,

    #[arg(long = "replicaof")]
    replica_host: Option<String>,
}

fn main() {
    let server = Server::new();
    let listener = server.listen().unwrap();

    // Periodically sweep store and remove expired entries
    let ctx = server.clone_ctx();
    thread::spawn(move || {
        loop {
            thread::sleep(STORE_SWEEP_INTERVAL);

            {
                let mut store = ctx.inner.store.write();
                store.sweep();
            }
        }
    });

    // Handle incoming connections
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                server.handle_stream(stream);
            }
            Err(e) => {
                eprintln!("Client encountered an unknown error: {e}");
            }
        }
    }
}
