use std::{fs, io};
use std::convert::TryFrom;
use std::fs::{File, OpenOptions};
use std::path::PathBuf;

use log::{debug, error, info, warn};

use crate::memtable::{ByteString, MemTable};
use crate::sstable::{Iter as SSTableIter, SSTable, SSTableMetadata};
use crate::wal::CommandLog;

const SSTABLE_MAX_LEVEL: usize = 5;

#[derive(Debug)]
pub struct KeyValuePair {
    pub key: ByteString,
    pub value: ByteString,
}

pub struct LSMStorage {
    base_path: String,
    memtable: MemTable<File>,
    sstables: Vec<Vec<SSTable<File>>>,
}

type ByteStr = [u8];

const FILE_SIZE_LIMIT: usize = 1024 * 4;

impl LSMStorage {
    pub fn load(base_path: String) -> io::Result<LSMStorage> {
        let path = PathBuf::from(&base_path);
        let mut levels = Vec::with_capacity(SSTABLE_MAX_LEVEL);
        for i in 0..SSTABLE_MAX_LEVEL {
            let mut level_path = path.clone();
            level_path.push(format!("level-{}", i));
            fs::create_dir_all(&level_path);

            let mut tables: Vec<SSTable<File>> = Vec::new();
            let paths = fs::read_dir(level_path)?;

            for path in paths {
                let path = path.expect("valid path in directory");
                match path.file_name().to_str() {
                    Some(name) => if name.contains("metadata") {
                        let sstable = SSTable::load(&path.path())?;
                        tables.push(sstable);
                    },
                    None => ()
                }
            }
            tables.sort();
            levels.push(tables);
        }
        let mut wal_path = path.clone();
        wal_path.push("wal");
        fs::create_dir_all(&wal_path);
        wal_path.push("wal.log");
        let memtable = match CommandLog::new(wal_path) {
            Ok(log) => MemTable::try_from(log)?,
            Err(err) => {
                warn!("Can't load wal log");
                MemTable::new(&base_path)
            }
        };
        Ok(LSMStorage {
            base_path,
            memtable,
            sstables: levels,
        })
    }

    pub fn insert(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<()> {
        debug!("Inserting key: {:?} ", key);
        self.memtable.insert(key.to_vec(), value.to_vec());
        if self.memtable.size_in_bytes() >= FILE_SIZE_LIMIT {
            debug!("Memtable is too big, creating new sstable");
            let mut sstable: SSTable<File> = SSTable::from_memtable(&self.base_path, &self.memtable).expect("Can't create new sstable");
            self.memtable.close().expect("Can't remove old wal log");
            self.sstables[0].push(sstable);
            self.memtable = MemTable::new(&self.base_path)
        }
        self.compact()?;
        Ok(())
    }

    fn compact(&mut self) -> io::Result<()> {
        for i in 0..SSTABLE_MAX_LEVEL - 1 {
            if self.sstables[i].len() >= 4 {
                let new_sstable = SSTable::merge_compact(&mut self.sstables[i], u8::try_from(i + 1).unwrap(), &self.base_path)?;
                self.sstables[i + 1].push(new_sstable);
                for table in &self.sstables[i] {
                    table.close();
                }
                self.sstables[i].clear();
            }
        }
        Ok(())
    }

    pub fn get(&mut self, key: &ByteStr) -> io::Result<Option<ByteString>> {
        let key_owned = key.to_vec();
        match self.memtable.get(&key_owned) {
            Some(val) => {
                debug!("Key: {:?} found in memtable", key);
                return Ok(Some(val.to_owned()));
            }
            None => {
                for i in 0..SSTABLE_MAX_LEVEL {
                    let mut level = &mut self.sstables[i];
                    for mut sstable in level.iter_mut().rev() {
                        match sstable.get(&key_owned)? {
                            Some(val) => {
                                debug!("Key: {:?} found in level {}, sstable: {}", key, i, sstable.id());
                                return Ok(Some(val));
                            }
                            None => () //do nothing
                        }
                    }
                }
            }
        }
        debug!("Key: {:?} is not found", key);
        Ok(None)
    }

    #[inline]
    pub fn update(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<()> {
        self.insert(key, value)
    }
    #[inline]
    pub fn delete(&mut self, key: &ByteStr) -> io::Result<()> {
        self.insert(key, &[0])
    }
}

#[cfg(test)]
mod tests {
    use std::{env, fs, io};

    use crate::lsm_storage::LSMStorage;
    use crate::memtable::MemTable;
    use crate::sstable::SSTable;

    #[test]
    fn storage_insert_test() -> io::Result<()> {
        let mut buf = env::temp_dir();
        buf.push("storage_test");
        let base_dir = buf.to_str().expect("Can't get temp directory");
        fs::remove_dir_all(base_dir)?;
        fs::create_dir_all(base_dir)?;
        let mut storage = LSMStorage::load(base_dir.to_string())?;
        for i in 0..10000 {
            storage.insert(&i.to_string().into_bytes(), &(i * 100).to_string().into_bytes())?;
        }
        for i in 0..10000 {
            let val = storage.get(&i.to_string().into_bytes())?;
            assert_eq!(Some((i * 100).to_string().into_bytes()), val);
        }
        assert_eq!(None, storage.get(&"20000".to_string().into_bytes())?);
        Ok(())
    }
}