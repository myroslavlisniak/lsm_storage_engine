use crate::ByteStr;
use std::collections::{btree_map, BTreeMap};
use std::io::{Read, Write};

use crate::wal::{CommandLog, LogRecord, WalError};

pub type ByteString = Vec<u8>;

pub struct MemTable {
    data: BTreeMap<ByteString, ByteString>,
    bytes: usize,
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}
impl MemTable {
    pub fn new() -> MemTable {
        MemTable {
            data: BTreeMap::new(),
            // wal: log,
            bytes: 0,
        }
    }

    pub fn from_log<T: Read + Write>(log: &mut CommandLog<T>) -> Result<MemTable, WalError> {
        let mut data = BTreeMap::new();
        let mut size: usize = 0;
        for res in log {
            let record = res?;
            match record {
                LogRecord::Insert(key, val) => {
                    let key_len = key.len();
                    let val_len = val.len();
                    data.insert(key, val);
                    size += key_len + val_len;
                }
                LogRecord::Remove(key) => {
                    let val = data.remove(&key);
                    size -= val.as_ref().map(|v| v.len() + key.len()).unwrap_or(0)
                }
            }
        }
        Ok(MemTable { data, bytes: size })
    }
}

impl IntoIterator for MemTable {
    type Item = (ByteString, ByteString);
    type IntoIter = btree_map::IntoIter<ByteString, ByteString>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
    }
}
impl<'a> IntoIterator for &'a MemTable {
    type Item = (&'a ByteString, &'a ByteString);
    type IntoIter = btree_map::Iter<'a, ByteString, ByteString>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.iter()
    }
}

impl MemTable {
    pub fn get(&self, key: &ByteStr) -> Option<&ByteString> {
        self.data.get(key)
    }

    pub fn insert(&mut self, key: ByteString, val: ByteString) -> Option<ByteString> {
        let key_len = key.len();
        let val_len = val.len();
        let prev = self.data.insert(key, val);
        let prev_val_size = prev.as_ref().map(|v| v.len() + key_len).unwrap_or(0);
        self.bytes = self.bytes + key_len + val_len - prev_val_size;
        prev
    }

    pub fn remove(&mut self, key: &ByteStr) -> Option<ByteString> {
        let prev = self.data.remove(key);
        let prev_size = prev.as_ref().map(|v| v.len() + key.len()).unwrap_or(0);
        self.bytes -= prev_size;
        prev
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }

    pub fn size_in_bytes(&self) -> usize {
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Cursor;

    use crate::memtable::MemTable;
    use crate::wal::{CommandLog, LogRecord};

    impl MemTable {
        pub fn new_in_memory_log() -> MemTable {
            MemTable {
                data: BTreeMap::new(),
                bytes: 0,
            }
        }
    }
    #[test]
    fn restore_from_log() {
        let mut log: CommandLog<Cursor<Vec<u8>>> = CommandLog::new_in_memory(Vec::new());
        let records = vec![
            LogRecord::Insert("key".as_bytes().to_vec(), "value".as_bytes().to_vec()),
            LogRecord::Insert("key1".as_bytes().to_vec(), "value1".as_bytes().to_vec()),
            LogRecord::Insert("key2".as_bytes().to_vec(), "value2".as_bytes().to_vec()),
            LogRecord::Remove("key2".as_bytes().to_vec()),
        ];

        for record in records {
            log.log(&record).unwrap();
        }
        let vec = log.inner();
        let mut log = CommandLog::new_in_memory(vec);

        let table = MemTable::from_log(&mut log).unwrap();
        assert_eq!(
            table.get("key1".as_bytes().to_vec().as_ref()),
            Some("value1".as_bytes().to_vec().as_ref())
        );
    }

    #[test]
    fn size_after_insert() {
        let mut table: MemTable = MemTable::new_in_memory_log();
        table.insert("key".as_bytes().to_vec(), "value".as_bytes().to_vec());
        assert_eq!(8, table.size_in_bytes());
        table.remove(&"key1".as_bytes().to_vec());
        assert_eq!(8, table.size_in_bytes());
        table.insert("key".as_bytes().to_vec(), "v".as_bytes().to_vec());
        assert_eq!(4, table.size_in_bytes());
        table.remove(&"key".as_bytes().to_vec());
        assert_eq!(0, table.size_in_bytes());
    }
}
