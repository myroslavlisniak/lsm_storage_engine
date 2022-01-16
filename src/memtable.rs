use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::fs::File;
use std::io;
use std::io::{Cursor, Read, Write};
use std::path::PathBuf;

use crate::wal::CommandLog;
use crate::wal::LogRecord;

pub type ByteString = Vec<u8>;

pub struct MemTable<T: Read + Write> {
    data: BTreeMap<ByteString, ByteString>,
    wal: CommandLog<T>,
    bytes: usize,
}

impl MemTable<File>{
    pub fn new() -> MemTable<File> {
        let log: CommandLog<File> = CommandLog::new(PathBuf::from("./wal.log"));
        MemTable {
            data: BTreeMap::new(),
            wal: log,
            bytes: 0,
        }
    }
}
impl MemTable<Cursor<Vec<u8>>> {
    pub fn new() -> MemTable<Cursor<Vec<u8>>> {
        let log: CommandLog<Cursor<Vec<u8>>> = CommandLog::new_in_memory(Vec::new());
        MemTable {
            data: BTreeMap::new(),
            wal: log,
            bytes: 0,
        }
    }
}
impl<T: Read + Write> MemTable<T> {


    pub fn get(&self, key: &ByteString) -> Option<&ByteString> {
        self.data.get(key)
    }

    pub fn insert(&mut self, key: ByteString, val: ByteString) -> Option<ByteString> {
        let key_len = key.len();
        let val_len = val.len();
        let prev = self.data.insert(key, val);
        let prev_val_size = prev.as_ref().map(|v| v.len()).unwrap_or(0);
        self.bytes += key_len + val_len - prev_val_size;
        prev
    }

    pub fn remove(&mut self, key: &ByteString) -> Option<ByteString> {
        self.wal.remove(key).expect("Can't write to WAL log");
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

impl<T: Read + Write> TryFrom<CommandLog<T>> for MemTable<T> {
    type Error = io::Error;

    fn try_from(mut wal: CommandLog<T>) -> Result<Self, Self::Error> {
        let mut data = BTreeMap::new();
        let mut size: usize = 0;
        for res in &mut wal {
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
        Ok(MemTable {
            data,
            wal,
            bytes: size,
        })
    }
}


#[cfg(test)]
mod tests {
    use std::convert::TryFrom;
    use std::io;
    use std::io::Cursor;

    use crate::memtable::MemTable;
    use crate::wal::{CommandLog, LogRecord};

    #[test]
    fn restore_from_log() -> io::Result<()> {
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
        let log= CommandLog::new_in_memory(vec);

        let table = MemTable::try_from(log)?;
        assert_eq!(table.get("key1".as_bytes().to_vec().as_ref()), Some("value1".as_bytes().to_vec().as_ref()));
        Ok(())
    }
}