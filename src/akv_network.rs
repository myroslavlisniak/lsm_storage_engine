extern crate libactionkv;
extern crate log4rs;
use libactionkv::Storage;
use libactionkv::command::Command;
use std::thread;
use std::net::{TcpListener, TcpStream, Shutdown};
use std::io::{BufRead, BufReader, Write};
use libactionkv::config::Config;
use log::{info, error, warn, debug};

fn handle_client(mut stream: TcpStream) {
    let config = Config::new().unwrap();

    let mut store = Storage::open(config).expect("unable open file");
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut data = String::new();

    while match reader.read_line(&mut data) {
        Ok(_) => {
            let cmd = Command::parse(&data);
            match cmd {
                Command::Get(key) => {
                    match store.get(&key).unwrap() {
                        None => write!(stream, "{:?} not found\n", String::from_utf8_lossy(&key)),
                        Some(value) => write!(stream, "{}\n", String::from_utf8_lossy(&value)),
                    }
                },
                Command::Delete(key) => {
                    store.delete(&key).unwrap();
                    write!(stream, "ok\n")
                },
                Command::Insert(key, value) => {
                    store.insert(&key, &value).unwrap();
                    write!(stream, "ok\n")
                },
                Command::Update(key, value) => {
                    store.update(&key, &value).unwrap();
                    write!(stream, "ok\n")
                },
                _ => write!(stream, "{}\n", "Supported commands: get, insert, update, delete")
            };
            data.clear();
            true
        },
        Err(_) => {
            println!("An error occurred, terminating connection with {}", stream.peer_addr().unwrap());
            stream.shutdown(Shutdown::Both).unwrap();
            false
        }
    } {}
}

fn main() {
    log4rs::init_file("config/log4rs.yaml", Default::default()).unwrap();
    let listener = TcpListener::bind("0.0.0.0:3333").unwrap();
    // accept connections and process them, spawning a new thread for each one
    info!("Server listening on port 3333");
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                info!("New connection: {}", stream.peer_addr().unwrap());
                thread::spawn(move|| {
                    // connection succeeded
                    handle_client(stream)
                });
            }
            Err(e) => {
                error!("Error: {}", e);
                /* connection failed */
            }
        }
    }
    // close the socket server
    drop(listener);
}