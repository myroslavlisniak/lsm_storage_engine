extern crate storage_engine;
extern crate log4rs;

// use std::net::Shutdown;
use std::sync::{Arc, Mutex};

use log::{error, info};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

use command::{Command, Response};
use command::Response::{Found, NotFound, NotSupported};
use storage_engine::config::Config;
use storage_engine::LsmStorage;

mod command;

async fn handle_client(stream: TcpStream, db: Arc<Mutex<LsmStorage>>) {
    let mut stream = BufReader::new(stream);
    let mut data = String::new();

    while match stream.read_line(&mut data).await {
        Ok(_) => {
            let cmd = Command::parse(&data);

            let result = match cmd {
                Command::Get(key) => {
                    let mut store = db.lock().unwrap();
                    match store.get(&key).unwrap() {
                        None => NotFound(key),
                        Some(value) => Found(value),
                    }
                }
                Command::Delete(key) => {
                    let mut store = db.lock().unwrap();
                    store.delete(&key).unwrap();
                    Response::Ok
                }
                Command::Insert(key, value) => {
                    let mut store = db.lock().unwrap();
                    store.insert(key, value).unwrap();
                    Response::Ok
                }
                Command::Update(key, value) => {
                    let mut store = db.lock().unwrap();
                    store.update(key, value).unwrap();
                    Response::Ok
                }
                _ => NotSupported,
            };
            match result {
                Response::Ok => stream.write_all("ok\n".as_bytes()).await,
                Response::Found(v) => stream.write_all(format!("{}\n", String::from_utf8_lossy(&v)).as_bytes()).await,
                Response::NotFound(key) => stream.write_all(format!("{} not found\n", String::from_utf8_lossy(&key)).as_bytes()).await,
                Response::NotSupported => stream.write_all(format!("{}\n", "Supported commands: get, insert, update, delete").as_bytes()).await
            }.unwrap_or_else(|e| error!("Error occurred {}", e));
            data.clear();
            true
        }
        Err(_) => {
            println!("An error occurred, terminating connection with {}", stream.get_ref().peer_addr().unwrap());
            stream.get_mut().shutdown().await.expect("Can't terminate server");
            false
        }
    } {}
}

#[tokio::main]
async fn main() {
    log4rs::init_file("config/log4rs.yaml", Default::default()).unwrap();
    let listener = TcpListener::bind("0.0.0.0:3333").await.unwrap();
    let config = Config::new().unwrap();
    let store = LsmStorage::load(config).expect("unable run storage");
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