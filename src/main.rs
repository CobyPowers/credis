#[macro_use]
mod resp;
mod arguments;
mod commands;
mod condvar;
mod server;
mod store;

use std::{thread, time::Duration};

use crate::server::Server;

const STORE_SWEEP_INTERVAL: Duration = Duration::from_secs(45);

fn main() {
    let server = Server::new();
    let listener = server.listen().unwrap();

    // If node is a replica, establish connection to master
    if !server.is_master() {
        let stream = server.connect_master().unwrap();
        server.handle_master_stream(stream);
    }

    // Periodically sweep store and remove expired entries
    let ctx = server.clone_ctx();
    thread::spawn(move || {
        loop {
            thread::sleep(STORE_SWEEP_INTERVAL);
            ctx.inner.store.write().sweep();
        }
    });

    // Handle client connections
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => server.handle_listener_stream(stream),
            Err(e) => eprintln!("Client encountered an unknown error: {e}"),
        };
    }
}
