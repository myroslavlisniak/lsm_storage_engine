extern crate byteorder;
extern crate config as config_lib;
extern crate crc;
extern crate serde_derive;

pub use lsm_storage::LsmStorage;

pub type ByteString = Vec<u8>;
pub type ByteStr = [u8];

pub mod config;
mod memtable;
mod wal;
mod sstable;
mod lsm_storage;
