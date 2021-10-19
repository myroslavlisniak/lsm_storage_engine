extern crate libactionkv;

use libactionkv::Storage;
use std::io::stdin;
use libactionkv::command::Command;
use libactionkv::config::Config;
use crate::Command::{Delete, Get, Insert, Update};



fn main() {
    let stdin = stdin();
    let config = Config::new().unwrap();
    // let action = args.get(2).expect(&USAGE).as_ref();
    // let key = args.get(3).expect(&USAGE).as_ref();
    // let maybe_value = args.get(4);

    let mut store = Storage::open(config).expect("unable open file");
    // store.load().expect("unable load data");
    loop {

        let mut command = String::new();
        stdin.read_line(&mut command);
        let cmd = Command::parse(&command);
        match cmd {
            Get(key) => {
                match store.get(&key).unwrap() {
                    None => eprintln!("{:?} not found", String::from_utf8_lossy(&key)),
                    Some(value) => println!("{:?}", String::from_utf8_lossy(&value)),
                }
            },
            Delete(key) => store.delete(&key).unwrap(),
            Insert(key, value) => {
                store.insert(&key, &value).unwrap()
            },
            Update(key, value) => {
                store.update(&key, &value).unwrap()
            },
            _ => eprintln!("{}", "Supported commands: get, insert, update, delete")
        }
    }
}