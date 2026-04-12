#[macro_use]
mod resp;
mod arguments;
mod commands;
mod condvar;
mod server;
mod store;

use std::{thread, time::Duration};

use crate::{
    resp::{RespKind, RespParser},
    server::Server,
};

const STORE_SWEEP_INTERVAL: Duration = Duration::from_secs(45);

fn main() {
    let server = Server::new();
    let listener = server.listen().unwrap();

    if !server.is_master() {
        let stream = server.connect_replica().unwrap();
        let mut rp = RespParser::new(&stream);
        rp.encode(&resp_arr!(vec![resp_bstr!("PING")])).unwrap();
    }

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
