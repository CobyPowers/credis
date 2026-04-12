#[macro_use]
mod resp;
mod arguments;
mod commands;
mod condvar;
mod store;

use std::{
    env,
    io::{BufReader, BufWriter},
    net::TcpListener,
    thread,
    time::Duration,
};

use crate::{
    commands::{CommandError, CommandHandler, SharedCommandContext},
    resp::RespParser,
};

const STORE_SWEEP_INTERVAL: Duration = Duration::from_secs(30);

fn main() {
    let mut port = 6379;

    let args: Vec<_> = env::args().collect();
    let first_arg = args.get(1);
    let second_arg = args.get(2);
    if first_arg.is_some_and(|x| x == "--port" || x == "-p") {
        if let Some(port_str) = second_arg
            && let Ok(port_val) = port_str.parse()
        {
            port = port_val;
        }
    }

    let listener = TcpListener::bind(("127.0.0.1", port)).unwrap();
    println!("Listening on 127.0.0.1:{}", port);

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
                        match handler.parse() {
                            Ok(_) => {}
                            Err(CommandError::ParseError(err)) => {
                                eprintln!("Error: Failed to parse input: `{:?}`", err);
                            }
                            Err(CommandError::ArgumentError(err)) => {
                                eprintln!("Error: Expected argument `{:?}`", err);
                            }
                            Err(CommandError::InvalidCommand(cmd)) => {
                                eprintln!("Error: Received invalid command `{cmd}`");
                            }
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
