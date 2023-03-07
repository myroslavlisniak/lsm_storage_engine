extern crate byteorder;
extern crate config as config_lib;
extern crate crc;
extern crate serde_derive;

pub use sync::lsm_storage::LsmStorage;
pub use crate::tokio::db::Db;

pub type ByteString = Vec<u8>;
pub type ByteStr = [u8];

pub mod config;
mod memtable;
mod wal;
mod sync;
mod tokio;
mod datafile;
mod sstable_metadata;

#[derive(Debug)]
pub struct KeyValuePair {
    pub key: ByteString,
    pub value: ByteString,
}

impl KeyValuePair {
    pub fn new(key: ByteString, value: ByteString) -> KeyValuePair {
        KeyValuePair{
            key,
            value
        }
    }
}
