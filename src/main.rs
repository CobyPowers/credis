use std::{
    io::{BufRead, BufReader, BufWriter, Write},
    net::TcpListener,
    thread,
};

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(move || {
                    let mut reader = BufReader::new(&stream);
                    let mut writer = BufWriter::new(&stream);

                    loop {
                        let mut buf = String::new();
                        reader.read_line(&mut buf).unwrap();

                        let cmd = match buf.find(' ') {
                            Some(i) => &buf[..i],
                            None => &buf.as_str(),
                        };
                        let args = &buf[cmd.len() - 1..];

                        if cmd.contains("PING") {
                            writer.write_all("+PONG\r\n".as_bytes()).unwrap();
                        } else if cmd.contains("ECHO") {
                            writer.write_all(args.as_bytes()).unwrap();
                        }

                        writer.flush().unwrap();
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
