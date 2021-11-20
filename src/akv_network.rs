extern crate libactionkv;
extern crate log4rs;
use libactionkv::Storage;
use libactionkv::command::{Command, Response};
use std::thread;
use tokio::net::{TcpListener, TcpStream};
use std::net::{Shutdown};
use std::io::{BufRead, Write};
use libactionkv::config::Config;
use log::{info, error, warn, debug};
use std::sync::{Arc, Mutex};
use tokio::io::{self, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use libactionkv::command::Response::{Found, NotFound, NotSupported};


async fn handle_client(mut stream: TcpStream, db: Arc<Mutex<Storage>>) {
    let mut stream = BufReader::new(stream);
    let mut data = String::new();

    while match stream.read_line(&mut data).await {
        Ok(_) => {
            let cmd = Command::parse(&data);

            let result = match cmd {
                Command::Get(key) => {
                    let store = db.lock().unwrap();
                    match store.get(&key).unwrap() {
                        None => NotFound(key),
                        Some(value) =>  Found(value),
                    }
                },
                Command::Delete(key) => {
                    let mut store = db.lock().unwrap();
                    store.delete(&key).unwrap();
                    Response::Ok
                },
                Command::Insert(key, value) => {
                    let mut store = db.lock().unwrap();
                    store.insert(&key, &value).unwrap();
                    Response::Ok
                },
                Command::Update(key, value) => {
                    let mut store = db.lock().unwrap();
                    store.update(&key, &value).unwrap();
                    Response::Ok
                },
                _ => NotSupported,
            };
            match result {
                Response::Ok => stream.write_all("ok\n".as_bytes()).await,
                Response::Found(v) => stream.write_all(format!("{}\n", String::from_utf8_lossy(&v)).as_bytes()).await,
                Response::NotFound(key) => stream.write_all(format!("{} not found\n", String::from_utf8_lossy(&key)).as_bytes()).await,
                Response::NotSupported => stream.write_all(format!("{}\n", "Supported commands: get, insert, update, delete").as_bytes()).await
            };
            data.clear();
            true
        },
        Err(_) => {
            println!("An error occurred, terminating connection with {}", stream.get_ref().peer_addr().unwrap());
            stream.get_mut().shutdown().await;
            false
        }
    } {}
}

#[tokio::main]
async fn main() {
    log4rs::init_file("config/log4rs.yaml", Default::default()).unwrap();
    let listener = TcpListener::bind("0.0.0.0:3333").await.unwrap();
    let config = Config::new().unwrap();
    let mut store = Storage::open(config).expect("unable open file");
    let db = Arc::new(Mutex::new(store));
    // accept connections and process them, spawning a new thread for each one
    info!("Server listening on port 3333");
    loop {
        let (socket, _) = listener.accept().await.unwrap();
        info!("New connection: {}", socket.peer_addr().unwrap());
        let db = db.clone();
        tokio::spawn(async move {
            handle_client(socket, db).await;
        });
    }
}