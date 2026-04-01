use std::{
    io::{BufRead, BufReader, BufWriter, Read, Write},
    net::TcpListener,
};

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let mut reader = BufReader::new(&stream);
                let mut writer = BufWriter::new(&stream);

                loop {
                    let mut buf = String::new();
                    reader.read_line(&mut buf).unwrap();
                    if buf.contains("PING") {
                        writer.write_all("+PONG\r\n".as_bytes()).unwrap();
                        writer.flush().unwrap();
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
