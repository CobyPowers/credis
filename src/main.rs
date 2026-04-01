use std::{
    io::{Read, Write},
    net::TcpListener,
};

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => loop {
                let mut buf = String::new();
                stream.read_to_string(&mut buf).unwrap();
                stream.write_all("+PONG\r\n".as_bytes()).unwrap();
            },
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
