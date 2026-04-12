use std::{
    io,
    net::{TcpListener, TcpStream},
    thread,
};

use clap::Parser;

use crate::{
    commands::{CommandError, CommandHandler, SharedCommandContext},
    resp::{RespKind, RespParser},
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
            Some(x) => ReplicationRole::Slave(x.replace(" ", ":")),
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

    pub fn is_master(&self) -> bool {
        let repl_data = self.ctx.inner.repl_data.read();
        match &repl_data.role {
            ReplicationRole::Master => true,
            ReplicationRole::Slave(_) => false,
        }
    }

    pub fn get_repl_addr(&self) -> Option<String> {
        let repl_data = self.ctx.inner.repl_data.read();
        match &repl_data.role {
            ReplicationRole::Master => None,
            ReplicationRole::Slave(x) => Some(x.clone()),
        }
    }

    pub fn listen(&self) -> io::Result<TcpListener> {
        let listener = TcpListener::bind((self.host, self.port));
        println!("Listening on {}:{}", self.host, self.port);
        listener
    }

    pub fn connect_replica(&self) -> io::Result<TcpStream> {
        match self.get_repl_addr() {
            Some(addr) => TcpStream::connect(addr),
            None => panic!("Attempted to connect to replica as master"),
        }
    }

    pub fn handle_listener_stream(&self, stream: TcpStream) {
        let ctx = self.ctx.clone();
        thread::spawn(move || {
            let rp = RespParser::new(&stream);
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

    pub fn handle_replica_stream(&self, stream: TcpStream) {
        let port = self.port;
        thread::spawn(move || {
            let mut rp = RespParser::new(&stream);
            rp.encode(&resp_arr!(vec![resp_bstr!("PING")])).unwrap();

            let res = rp.decode().unwrap();
            if res == RespKind::SimpleString("PONG".to_string()) {
                rp.encode(&resp_arr!(vec![
                    resp_bstr!("REPLCONF"),
                    resp_bstr!("listening-port"),
                    resp_bstr!(port)
                ]))
                .unwrap();

                rp.encode(&resp_arr!(vec![
                    resp_bstr!("REPLCONF"),
                    resp_bstr!("capa"),
                    resp_bstr!("psync2")
                ]))
                .unwrap();
            }
        });
    }

    pub fn clone_ctx(&self) -> SharedCommandContext {
        self.ctx.clone()
    }
}
