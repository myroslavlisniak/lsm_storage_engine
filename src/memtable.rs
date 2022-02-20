use std::io;
use std::collections::{btree_map, BTreeMap};
use std::convert::TryFrom;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{PathBuf};
use crate::ByteStr;


use crate::wal::CommandLog;
use crate::wal::LogRecord;

pub type ByteString = Vec<u8>;

pub struct MemTable<T: Read + Write> {
    data: BTreeMap<ByteString, ByteString>,
    wal: CommandLog<T>,
    bytes: usize,
}

impl MemTable<File> {
    pub fn new(base_path: &str) -> MemTable<File> {
        let mut path_buf = PathBuf::from(base_path);
        path_buf.push("wal");
        path_buf.push("wal.log");
        let log: CommandLog<File> = CommandLog::new(path_buf).unwrap();
        MemTable {
            data: BTreeMap::new(),
            wal: log,
            bytes: 0,
        }

    }

    pub fn close(&mut self) -> io::Result<()> {
        self.wal.close()
    }

}

impl<T: Read + Write> IntoIterator for MemTable<T> {
    type Item = (ByteString, ByteString);
    type IntoIter = btree_map::IntoIter<ByteString, ByteString>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
    }
}
impl<'a, T: Read + Write> IntoIterator for &'a MemTable<T> {
    type Item = (&'a ByteString, &'a ByteString);
    type IntoIter = btree_map::Iter<'a, ByteString, ByteString>;

    fn into_iter(self) -> Self::IntoIter {
        (&self.data).iter()
    }
}

impl<T: Read + Write> MemTable<T> {
    pub fn get(&self, key: &ByteStr) -> Option<&ByteString> {
        self.data.get(key)
    }

    pub fn insert(&mut self, key: ByteString, val: ByteString) -> Option<ByteString> {
        let key_len = key.len();
        let val_len = val.len();
        self.wal.insert(&key, &val).expect("Can't write insert to WAL log");
        let prev = self.data.insert(key, val);
        let prev_val_size = prev.as_ref().map(|v| v.len() + key_len).unwrap_or(0);
        self.bytes = self.bytes + key_len + val_len - prev_val_size;
        prev
    }

    pub fn remove(&mut self, key: &ByteStr) -> Option<ByteString> {
        self.wal.remove(key).expect("Can't write remove to WAL log");
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
    use std::collections::BTreeMap;
    use std::convert::TryFrom;
    use std::io;
    use std::io::Cursor;

    use crate::memtable::MemTable;
    use crate::wal::{CommandLog, LogRecord};
    type InMemoryFile = Cursor<Vec<u8>>;

    impl MemTable<InMemoryFile> {
        pub fn new_in_memory_log() -> MemTable<InMemoryFile> {
            let log: CommandLog<InMemoryFile> = CommandLog::new_in_memory(Vec::new());
            MemTable {
                data: BTreeMap::new(),
                wal: log,
                bytes: 0,
            }
        }
    }
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
        let log = CommandLog::new_in_memory(vec);

        let table = MemTable::try_from(log)?;
        assert_eq!(table.get("key1".as_bytes().to_vec().as_ref()), Some("value1".as_bytes().to_vec().as_ref()));
        Ok(())
    }

    #[test]
    fn size_after_insert() -> io::Result<()> {
        let mut table: MemTable<InMemoryFile> = MemTable::new_in_memory_log();
        table.insert("key".as_bytes().to_vec(), "value".as_bytes().to_vec());
        assert_eq!(8, table.size_in_bytes());
        table.remove(&"key1".as_bytes().to_vec());
        assert_eq!(8, table.size_in_bytes());
        table.insert("key".as_bytes().to_vec(), "v".as_bytes().to_vec());
        assert_eq!(4, table.size_in_bytes());
        table.remove(&"key".as_bytes().to_vec());
        assert_eq!(0, table.size_in_bytes());
        Ok(())
    }
}