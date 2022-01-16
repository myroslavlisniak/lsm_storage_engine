use std::collections::btree_map::IntoIter;
use std::collections::BTreeMap;
use crate::wal::CommandLog;

pub type ByteStr = [u8];
pub type ByteString = Vec<u8>;

pub struct MemTable {
    data: BTreeMap<ByteString, ByteString>,
    wal: CommandLog
}

impl MemTable {

    pub fn new() -> MemTable {
        MemTable{
            data: BTreeMap::new(),
            wal: CommandLog::new()
        }
    }

    pub fn get(&self, key: &ByteString) -> Option<&ByteString> {
        self.data.get(key)
    }

    pub fn insert(&mut self, key: ByteString, val: ByteString) -> Option<ByteString> {
        // self.wal.insert(key, val);
        self.data.insert(key, val)
    }

    pub fn remove(&mut self, key: &ByteString) -> Option<ByteString> {
        self.data.remove(key)
    }
}

