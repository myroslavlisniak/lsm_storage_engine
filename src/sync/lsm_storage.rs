use std::{fs, io};
use std::convert::TryFrom;
use std::fs::File;
use std::path::PathBuf;

use log::{debug};

use crate::{ByteStr, ByteString};
use crate::config::Config;
use crate::memtable::MemTable;
use crate::sync::sstable::SsTable;
use crate::wal::CommandLog;

const SSTABLE_MAX_LEVEL: usize = 5;


pub struct LsmStorage {
    config: Config,
    wal: CommandLog<File>,
    memtable: MemTable,
    sstables: Vec<Vec<SsTable>>,
}

impl LsmStorage {
    pub fn load(config: Config) -> io::Result<LsmStorage> {
        let path = PathBuf::from(&config.base_path);
        let mut levels = Vec::with_capacity(SSTABLE_MAX_LEVEL);
        for i in 0..SSTABLE_MAX_LEVEL {
            let mut level_path = path.clone();
            level_path.push(format!("level-{}", i));
            fs::create_dir_all(&level_path)?;

            let mut tables: Vec<SsTable> = Vec::new();
            let paths = fs::read_dir(level_path)?;

            for path in paths {
                let path = path.expect("valid path in directory");
                if let Some(name) =  path.file_name().to_str() {
                    if name.contains("metadata") {
                        let sstable = SsTable::load(&path.path())?;
                        tables.push(sstable);
                    }
                }
            }
            tables.sort();
            levels.push(tables);
        }
        let wal_path = LsmStorage::wal_path(&config.base_path);
        let mut command_log = CommandLog::new(wal_path)?;
        let memtable = MemTable::from_log(&mut command_log)
            .expect("Can't restore memtable from a log");
        Ok(LsmStorage {
            config,
            wal: command_log,
            memtable,
            sstables: levels,
        })
    }

    pub fn insert(&mut self, key: ByteString, value: ByteString) -> io::Result<()> {
        debug!("Inserting key: {:?} ", key);
        self.wal.insert(&key, &value).expect("Can't write command to WAL log");
        self.memtable.insert(key, value);
        if self.memtable.size_in_bytes() >= self.config.memtable_limit_bytes {
            debug!("Memtable is too big, creating new sstable");
            let sstable: SsTable = SsTable::from_memtable(&self.config.base_path, &self.memtable)
                .expect("Can't create new sstable");
            self.wal.close().expect("Can't remove old wal log");
            self.sstables[0].push(sstable);
            self.wal = CommandLog::new(LsmStorage::wal_path(&self.config.base_path))
                .expect("Can't create WAL file");
            self.memtable = MemTable::new();
            self.compact()?;
        }

        Ok(())
    }
    fn wal_path(base_path: &str) -> PathBuf {
        let mut  wal_path = PathBuf::from(base_path);
        wal_path.push("wal");
        wal_path.push("wal.log");
        wal_path
    }

    pub fn get(&mut self, key: &ByteStr) -> io::Result<Option<ByteString>> {
        match self.get_internal(key) {
            Ok(Some(val)) => {
                if val == vec![0]{
                    Ok(None)
                } else {
                    Ok(Some(val))
                }
            },
            res => res
        }
    }

    fn get_internal(&mut self, key: &ByteStr) -> io::Result<Option<ByteString>> {
        // let key_owned = key.to_vec();
        match self.memtable.get(key) {
            Some(val) => {
                debug!("Key: {:?} found in memtable", key);
                return Ok(Some(val.to_owned()));
            }
            None => {
                for i in 0..SSTABLE_MAX_LEVEL {
                    let level = &mut self.sstables[i];
                    for sstable in level.iter_mut().rev() {
                        if let Some(val) = sstable.get(key)? {
                            debug!("Key: {:?} found in level {}, sstable: {}", key, i, sstable.id());
                            return Ok(Some(val));
                        }
                    }
                }
            }
        }
        debug!("Key: {:?} is not found", key);
        Ok(None)
    }

    #[inline]
    pub fn update(&mut self, key: ByteString, value: ByteString) -> io::Result<()> {
        self.insert(key, value)
    }

    #[inline]
    pub fn delete(&mut self, key: &ByteStr) -> io::Result<()> {
        self.wal.remove(key).expect("Can't write command to WAL log");
        self.memtable.insert(key.to_vec(), vec![0]);
        Ok(())
    }

    fn compact(&mut self) -> io::Result<()> {
        for i in 0..SSTABLE_MAX_LEVEL - 1 {
            if self.sstables[i].len() >= self.config.sstable_level_limit {
                let new_sstable = SsTable::merge_compact(&mut self.sstables[i], u8::try_from(i + 1).unwrap(), &self.config.base_path)?;
                self.sstables[i + 1].push( new_sstable);
                for table in &self.sstables[i] {
                    table.close()?;
                }
                self.sstables[i].clear();
            }
        }
        Ok(())
    }




}

#[cfg(test)]
mod tests {
    use std::{env, fs, io};
    use std::collections::HashMap;

    use rand::Rng;
    use serial_test::serial;

    use crate::config::Config;
    use crate::LsmStorage;

    fn prepare_directories() -> String{
        let mut buf = env::temp_dir();
        buf.push("storage_test");
        let base_dir = buf.to_str().expect("Can't get temp directory");
        fs::remove_dir_all(base_dir).unwrap_or(());
        fs::create_dir_all(base_dir).unwrap();
        base_dir.to_string()
    }

    #[test]
    #[serial]
    fn storage_insert_test() -> io::Result<()> {
        let base_dir = prepare_directories();

        let config = Config {
            base_path: base_dir.to_string(),
            memtable_limit_bytes: 4096,
            sstable_level_limit: 4
        };
        let mut storage = LsmStorage::load(config)?;
        for i in 0..10000 {
            storage.insert(i.to_string().into_bytes(), (i * 100).to_string().into_bytes())?;
        }
        for i in 0..10000 {
            let val = storage.get(&i.to_string().into_bytes())?;
            assert_eq!(Some((i * 100).to_string().into_bytes()), val);
        }
        assert_eq!(None, storage.get(&"20000".to_string().into_bytes())?);
        Ok(())
    }

    #[test]
    #[serial]
    fn storage_compact_test() -> io::Result<()> {
        let mut rng = rand::thread_rng();

        let mut hash_map = HashMap::new();
        let base_dir = prepare_directories();
        let config = Config {
            base_path: base_dir.to_string(),
            memtable_limit_bytes: 4096,
            sstable_level_limit: 4
        };
        let mut storage = LsmStorage::load(config)?;
        for _i in 0..100000 {
            let key: u32 = rng.gen::<u32>() % 500;
            let key = format!("kt_{}", key).into_bytes();
            let val = format!("vt_{}", rng.gen::<u32>()).into_bytes();
            hash_map.insert(key.clone(), val.clone());
            storage.insert(key.clone(), val.clone()).unwrap();
            let option = storage.get(&key).unwrap();
            assert_eq!(val, option.unwrap());
        }
        for (key, val) in &hash_map {
            assert_eq!(*val, storage.get(key).unwrap().unwrap());
        }

        Ok(())
    }
}
