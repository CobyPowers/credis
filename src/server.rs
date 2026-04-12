use std::{
    io::{self, BufReader, BufWriter},
    net::{TcpListener, TcpStream},
    thread,
};

use clap::Parser;

use crate::{
    commands::{CommandError, CommandHandler, SharedCommandContext},
    resp::RespParser,
};

#[derive(Default)]
pub enum ReplicationRole {
    #[default]
    Master,
    Slave(String), // Master host
}

pub struct ReplicationData {
    pub role: ReplicationRole,
    pub id: String,
    pub offset: usize,
}

impl Default for ReplicationData {
    fn default() -> Self {
        Self {
            role: Default::default(),
            id: String::from("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"),
            offset: Default::default(),
        }
    }
}

#[derive(Debug, Parser)]
pub struct ServerArgs {
    #[arg(short, long, default_value_t = 6379)]
    pub port: u16,

    #[arg(long = "replicaof")]
    pub replica_host: Option<String>,
}

pub struct Server {
    pub host: &'static str,
    pub port: u16,
    ctx: SharedCommandContext,
}

impl Server {
    pub fn new() -> Self {
        let args = ServerArgs::parse();

        let role = match args.replica_host {
            Some(x) => ReplicationRole::Slave(x),
            None => ReplicationRole::Master,
        };

        let ctx = SharedCommandContext::default();
        {
            let mut repl_data = ctx.inner.repl_data.write();
            repl_data.role = role;
        }

        Self {
            host: "127.0.0.1",
            port: args.port,
            ctx: ctx,
        }
    }

    pub fn listen(&self) -> io::Result<TcpListener> {
        let listener = TcpListener::bind((self.host, self.port));
        println!("Listening on {}:{}", self.host, self.port);
        listener
    }

    pub fn handle_stream(&self, stream: TcpStream) {
        let ctx = self.ctx.clone();
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

    pub fn clone_ctx(&self) -> SharedCommandContext {
        self.ctx.clone()
    }
}
