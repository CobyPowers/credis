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

#[derive(Debug, Clone)]
pub enum ReplicationRole {
    Master,
    Slave(String), // Master host
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
    pub role: ReplicationRole,
    ctx: SharedCommandContext,
}

impl Server {
    pub fn new() -> Self {
        let args = ServerArgs::parse();

        let role = args
            .replica_host
            .and_then(|x| Some(ReplicationRole::Slave(x)))
            .or_else(|| Some(ReplicationRole::Master))
            .unwrap();

        Self {
            host: "127.0.0.1",
            port: args.port,
            role: role,
            ctx: SharedCommandContext::default(),
        }
    }

    pub fn listen(&self) -> io::Result<TcpListener> {
        let listener = TcpListener::bind((self.host, self.port));
        println!("Listening on {}:{}", self.host, self.port);
        listener
    }

    pub fn handle_stream(&self, stream: TcpStream) {
        let ctx = self.ctx.clone();
        let role = self.role.clone();

        thread::spawn(move || {
            let rp = RespParser::new(BufReader::new(&stream), BufWriter::new(&stream));
            let mut handler = CommandHandler::new(rp, ctx, role);

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
