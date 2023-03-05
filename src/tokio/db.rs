use std::{fs, io, mem};
use std::convert::TryFrom;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use log::{debug, info};
use parking_lot::RwLock;
use parking_lot::lock_api::RwLockUpgradableReadGuard;

use crate::{ByteStr, ByteString};
use crate::config::Config;
use crate::memtable::MemTable;
use crate::tokio::sstable::SsTable;
use crate::wal::CommandLog;

const SSTABLE_MAX_LEVEL: usize = 5;


#[derive(Clone)]
pub struct Db {
    state: Arc<State>,
}

struct State {
    config: Config,
    memtable: RwLock<MemTable>,
    old_memtable: RwLock<Option<Arc<MemTable>>>,
    wal: RwLock<CommandLog<File>>,
    levels: RwLock<SsLevelTable>,
}

struct SsLevelTable {
    levels: Vec<Vec<SsTable>>,
}



impl Db {
    pub fn load(config: Config) -> io::Result<Db> {
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
                if let Some(name) = path.file_name().to_str() {
                    if name.contains("metadata") {
                        let sstable = SsTable::load(&path.path())?;
                        tables.push(sstable);
                    }
                }
            }
            tables.sort();
            levels.push(tables);
        }
        let wal_path = Self::wal_path(&config.base_path);
        let mut command_log = CommandLog::new(wal_path)?;
        let memtable = MemTable::from_log(&mut command_log)
            .expect("Can't restore memtable from a log");
        Ok(Db {
            state: Arc::new(State {
                config,
                memtable: RwLock::new(memtable),
                old_memtable: RwLock::new(None),
                wal: RwLock::new(command_log),
                levels: RwLock::new(SsLevelTable {
                    levels,
                }),
            }),
        })
    }
    fn wal_path(base_path: &str) -> PathBuf {
        let mut wal_path = PathBuf::from(base_path);
        wal_path.push("wal");
        wal_path.push("wal.log");
        wal_path
    }
    //TODO make it await wal insert
    pub async fn insert(&self, key: ByteString, value: ByteString) -> io::Result<()> {
        {
            let mut wal = self.state.wal.write();
            wal.insert(&key, &value)?;
        }
        let mut old_clone = None;
        {
            let mut memtable = self.state.memtable.write();
            memtable.insert(key, value);
            let size = memtable.size_in_bytes();
            if size > self.state.config.memtable_limit_bytes {
                let mut old = self.state.old_memtable.write();
                if old.is_none() {
                    let old_table = mem::replace(&mut *memtable, MemTable::new());
                    let arc = Arc::new(old_table);
                    old_clone = Some(arc.clone());
                    *old = Some(arc);
                }
            }
        }
        if old_clone.is_some() {
            let state = self.state.clone();
            tokio::task::spawn_blocking(move || {
                debug!("Memtable is too big, creating new sstable");
                let sstable: SsTable = SsTable::from_memtable(&state.config.base_path, &old_clone.unwrap())
                    .expect("Can't create new sstable");
                {
                    let mut levels = state.levels.write();
                    levels.levels[0].push(sstable);
                }
                {
                    let mut wal = state.wal.write();
                    wal.close().expect("Can't remove old wal log");
                    *wal = CommandLog::new(Self::wal_path(&state.config.base_path))
                        .expect("Can't create WAL file");
                }
                {
                    let mut old = state.old_memtable.write();
                    *old = None
                }
            });
        }
        Ok(())
    }

    #[inline]
    pub async fn update(&self, key: ByteString, value: ByteString) -> io::Result<()> {
        self.insert(key, value).await
    }

    #[inline]
    pub async fn delete(&self, key: &ByteStr) -> io::Result<()> {
        {
            let mut wal = self.state.wal.write();
            wal.remove(key)?;
        }
        {
            let mut memtable = self.state.memtable.write();
            memtable.insert(key.to_vec(), vec![0]);
        }
        Ok(())
    }
    pub async fn get(&self, key: &ByteStr) -> io::Result<Option<ByteString>> {
        match self.get_internal(key).await {
            Ok(Some(val)) => {
                if val == vec![0] {
                    Ok(None)
                } else {
                    Ok(Some(val))
                }
            }
            res => res
        }
    }
    async fn get_internal(&self, key: &ByteStr) -> io::Result<Option<ByteString>> {
        // let key_owned = key.to_vec();
        {
            let memtable = self.state.memtable.read();
            let result = memtable.get(key);
            if let Some(val) = result {
                debug!("Key: {:?} found in memtable", key);
                return Ok(Some(val.to_owned()));
            }
        }
        {
            let old_memtable = self.state.old_memtable.read();
            let result = old_memtable.as_ref().and_then(|m| m.get(key));
            if let Some(val) = result {
                debug!("Key: {:?} found in memtable", key);
                return Ok(Some(val.to_owned()));
            }
        }
        let state = self.state.clone();
        let key = key.to_owned();
        let result = tokio::task::spawn_blocking(move || {
            let levels = state.levels.read();
            for i in 0..SSTABLE_MAX_LEVEL {
                for sstable in levels.levels[i].iter().rev() {
                    if let Some(val) = sstable.get(&key)? {
                        // debug!("Key: {:?} found in level {}, sstable: {}", key, i, sstable.id());
                        return Ok(Some(val));
                    }
                }
            }
            Ok(None)
        }).await?;


        // debug!("Key: {:?} is not found", key);
        result
    }

    pub async fn compact(&self) -> io::Result<()> {
        let db = self.clone();
        tokio::task::spawn_blocking(move || {
            info!("Compaction started");
            let mut new_levels: Vec<Vec<SsTable>> = Vec::new();
            for _ in 0..SSTABLE_MAX_LEVEL {
                new_levels.push(Vec::new());
            }
            {
                let levels = db.state.levels.upgradable_read();
                for i in 0..SSTABLE_MAX_LEVEL - 1 {
                    if levels.levels[i].len() >= db.state.config.sstable_level_limit {
                        info!("Compaction on level {}", i);
                        let new_sstable = SsTable::merge_compact(&levels.levels[i], u8::try_from(i + 1).unwrap(), &db.state.config.base_path)?;
                        new_levels[i + 1].push(new_sstable);
                        // FIXME
                        for table in &levels.levels[i] {
                            table.close()?;
                        }
                    } else {
                        for j in 0..levels.levels[i].len() {
                            new_levels[i].push(levels.levels[i][j].clone());
                        }
                        // new_levels[i] = levels.levels[i].clone();
                    }
                }
                let mut levels = RwLockUpgradableReadGuard::upgrade(levels);
                *levels = SsLevelTable {
                    levels: new_levels
                };
            }
            info!("Compaction finished");
            Ok(())
        }).await?

    }

}


#[cfg(test)]
mod tests {
    use std::{env, fs, io};
    use std::collections::HashMap;

    use rand::Rng;

    use crate::config::Config;
    use crate::tokio::db::Db;

    fn prepare_directories() -> String{
        let mut buf = env::temp_dir();
        buf.push("storage_test");
        let base_dir = buf.to_str().expect("Can't get temp directory");
        fs::remove_dir_all(base_dir).unwrap_or(());
        fs::create_dir_all(base_dir).unwrap();
        base_dir.to_string()
    }

    #[tokio::test(flavor ="multi_thread", worker_threads = 8)]
    // #[serial]
    async fn storage_compact_test() -> io::Result<()> {
        let mut rng = rand::thread_rng();

        let mut hash_map = HashMap::new();
        let base_dir = prepare_directories();
        let config = Config {
            base_path: base_dir.to_string(),
            memtable_limit_bytes: 4096,
            sstable_level_limit: 4
        };
        let mut storage = Db::load(config)?;
        let db_clone = storage.clone();
        tokio::task::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(10));
            loop {
                interval.tick().await;
                db_clone.compact().await.expect("Compact failed");
            }
        });
        for _i in 0..100000 {
            let key: u32 = rng.gen::<u32>() % 500;
            let key = format!("kt_{}", key).into_bytes();
            let val = format!("vt_{}", rng.gen::<u32>()).into_bytes();
            hash_map.insert(key.clone(), val.clone());
            storage.insert(key.clone(), val.clone()).await.unwrap();
            let option = storage.get(&key).await.unwrap();
            assert_eq!(val, option.unwrap());
        }
        for (key, val) in &hash_map {
            assert_eq!(*val, storage.get(key).await.unwrap().unwrap());
        }

        Ok(())
    }
}
