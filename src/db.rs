use std::collections::vec_deque::VecDeque;
use std::fs::{File, OpenOptions};
use std::{fs, io, mem};
use std::cmp::Ordering;
use std::collections::btree_map::{BTreeMap, Range};
use std::convert::TryFrom;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::mem::take;
use std::path::{Path, PathBuf};
use std::sync::{Arc};
use std::time::{SystemTime, UNIX_EPOCH};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use log::{debug, info};
use probabilistic_collections::bloom::BloomFilter;
use sha2::{Digest, Sha256};
use tokio::sync::Notify;
use crate::{ByteStr, ByteString};
use crate::config::Config;
use crate::lsm_storage::KeyValuePair;
use crate::memtable::MemTable;
use crate::sstable::{SstableBloomFilter, SstableIndex};
use crate::wal::CommandLog;
use serde::{Deserialize, Serialize};
use base64::encode as base64_encode;
use parking_lot::{RwLock, Mutex};
use parking_lot::lock_api::RwLockUpgradableReadGuard;

const SSTABLE_MAX_LEVEL: usize = 5;
const INDEX_STEP: usize = 100;

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

#[derive(Serialize, Deserialize)]
struct Checksums {
    index_checksum: String,
    data_checksum: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SsTableMetadata {
    base_path: String,
    id: u128,
    level: u8,
    metadata_filename: String,
    checksum_filename: String,
    data_filename: String,
    index_filename: String,
    bloom_filter_filename: String,
}

struct SsTableMeta {
    metadata: SsTableMetadata,
    index: SstableIndex,
    bloom_filter: SstableBloomFilter,
    size_bytes: u64,
}

struct DataFile {
    data: File,
}

struct SsTable {
    meta: SsTableMeta,
    data: Mutex<VecDeque<DataFile>>,
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
        let mut size = 0;
        {
            let mut memtable = self.state.memtable.write();
            memtable.insert(key, value);
            size = memtable.size_in_bytes();
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
                    (*levels).levels[0].push(sstable);
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
            wal.remove(&key)?;
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

impl SsTable {
    pub fn load(metadata_path: &Path) -> io::Result<SsTable> {
        let metadata = SsTableMetadata::load(metadata_path);
        let calculated_data_hash = SsTable::calculate_checksum(&metadata.data_path())?;
        let calculated_index_hash = SsTable::calculate_checksum(&metadata.index_path())?;
        let checksum_file = OpenOptions::new()
            .read(true)
            .open(&metadata.checksum_path())
            .expect("Can't open checksum file");
        let checksums: Checksums = serde_json::from_reader(checksum_file).map_err(io::Error::from)?;
        if calculated_data_hash != checksums.data_checksum {
            panic!("Can't load SSTable from {}. Checksum is not correct", &metadata.data_filename);
        }
        if calculated_index_hash != checksums.index_checksum {
            panic!("Can't load SSTable from {}. Checksum is not correct", &metadata.index_filename);
        }
        let mut data_file = OpenOptions::new()
            .read(true)
            .open(&metadata.data_path())
            .expect("Can't create/open data file");
        let index_file = OpenOptions::new()
            .read(true)
            .open(&metadata.index_path())
            .expect("Can't create/open data file");
        let bloom_filter_file = OpenOptions::new()
            .read(true)
            .open(&metadata.bloom_filter_path())
            .expect("Cant'create/open bloom filter file");
        let index: BTreeMap<ByteString, u64> = bincode::deserialize_from(index_file)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;
        let bloom_filter: BloomFilter<ByteString> = bincode::deserialize_from(bloom_filter_file)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;
        let size = data_file.seek(SeekFrom::End(0))?;
        data_file.seek(SeekFrom::Start(0))?;
        let mut queue = VecDeque::new();
        //todo config
        for _ in 0..8 {
            queue.push_back(DataFile::open(metadata.data_path().as_path()).unwrap());
        }
        Ok(SsTable {
            meta: SsTableMeta {
                metadata,
                index,
                bloom_filter,
                size_bytes: size,
            },
            data: Mutex::new(queue),
        })
    }
    pub fn get(&self, key: &ByteStr) -> io::Result<Option<ByteString>> {
        if !self.meta.bloom_filter.contains(key) {
            return Ok(None);
        }
        let mut data = None;
        {
            let mut locked_data = self.data.lock().pop_front();
            mem::swap(&mut data, &mut locked_data);
        }
        // todo
        let mut data = data.unwrap();
        let result = match self.meta.index.get(key) {
            Some(pos) => {
                let position = *pos;
                data.read_record(position)
                    .map(|op| op.map(|p| p.0.value))
            }
            None => {
                let (start, end) = self.position_range(key);
                data.scan_range(key, start, end)
            }
        };
        {
            self.data.lock().push_back(data);
        }
        result
    }

    fn position_range(&self, key: &ByteStr) -> (u64, u64) {
        let mut range: Range<ByteString, u64> = self.meta.index.range::<ByteString, _>(..key.to_vec());
        let start = range.next_back().map(|r| *r.1).unwrap_or(0);
        let mut range: Range<ByteString, u64> = self.meta.index.range::<ByteString, _>(key.to_vec()..);
        let end = range.next().map(|e| *e.1).unwrap_or(self.meta.size_bytes);
        (start, end)
    }

    pub fn from_memtable(base_path: &str, memtable: &MemTable) -> io::Result<SsTable> {
        let metadata = SsTableMetadata::new(base_path.to_string(), 0);
        let (_, index, bloom_filter, size) =
            SsTable::write_data_file(&metadata, memtable)?;
        SsTable::write_index(&metadata, &index)?;
        SsTable::write_checksums(&metadata)?;
        SsTable::write_bloom_filter(&metadata, &bloom_filter)?;
        SsTable::write_metadata(&metadata)?;
        let mut queue = VecDeque::new();
        //todo config
        for _ in 0..8 {
            queue.push_back(DataFile::open(metadata.data_path().as_path()).unwrap());
        }
        let sstable_meta = SsTableMeta {
            metadata,
            index,
            bloom_filter,
            size_bytes: size,
        };

        Ok(SsTable {
            meta: sstable_meta,
            data: Mutex::new(queue),
        })
    }

    fn write_data_file(metadata: &SsTableMetadata, memtable: &MemTable) -> io::Result<(File, SstableIndex, SstableBloomFilter, u64)> {
        let mut data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&metadata.data_path())
            .unwrap_or_else(|_| panic!("Can't create/open data file {}", &metadata.metadata_filename));
        let mut index: BTreeMap<ByteString, u64> = BTreeMap::new();
        let mut bloom_filter = BloomFilter::<ByteString>::new(memtable.size(), 0.01);
        let mut pos = 0;
        for (i, (key, val)) in memtable.into_iter().enumerate() {
            let diff = SsTable::write_key_value(&mut data_file, key, val)?;
            bloom_filter.insert(key);
            if i % INDEX_STEP == 0 {
                index.insert(key.clone(), pos);
            }
            pos += diff;
        }
        data_file.flush()?;

        let data_file = OpenOptions::new()
            .read(true)
            .open(&metadata.data_path())?;
        Ok((data_file, index, bloom_filter, pos))
    }

    fn write_index(metadata: &SsTableMetadata, index: &BTreeMap<ByteString, u64>) -> io::Result<()> {
        let index_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&metadata.index_path())?;
        bincode::serialize_into(index_file, &index)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))
    }

    fn write_metadata(metadata: &SsTableMetadata) -> io::Result<()> {
        let metadata_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&metadata.metadata_path())?;
        serde_json::to_writer(metadata_file, metadata)
            .map_err(io::Error::from)
    }

    fn write_bloom_filter(metadata: &SsTableMetadata, bloom_filter: &BloomFilter<ByteString>) -> io::Result<()> {
        let bloom_filter_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&metadata.bloom_filter_path())?;
        bincode::serialize_into(bloom_filter_file, bloom_filter)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))
    }

    fn write_checksums(metadata: &SsTableMetadata) -> io::Result<()> {
        let data_base64_hash = SsTable::calculate_checksum(&metadata.data_path())?;
        let index_base64_hash = SsTable::calculate_checksum(&metadata.index_path())?;
        debug!("Base64-encoded hash: {}, for file: {}", data_base64_hash, &metadata.data_filename);
        let checksums = Checksums {
            index_checksum: index_base64_hash,
            data_checksum: data_base64_hash,
        };
        let checksum_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&metadata.checksum_path())?;
        serde_json::to_writer(checksum_file, &checksums)
            .map_err(io::Error::from)
    }
    fn calculate_checksum(path: &Path) -> io::Result<String> {
        let mut hasher = Sha256::new();
        let mut data_file = OpenOptions::new()
            .read(true)
            .open(path)
            .expect("Can't open file to calculate checksum");
        // let start = SeekFrom::Start(0);
        // data_file.seek(start)?;
        let mut buffer = [0; 1024];
        loop {
            let count = data_file.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            sha2::Digest::update(&mut hasher, &buffer[..count]);
        }
        let hash = hasher.finalize();
        Ok(base64_encode(&hash))
    }

    fn write_key_value(data_file: &mut File, key: &ByteStr, val: &ByteStr) -> io::Result<u64> {
        let key_len = key.len() as u32;
        let val_len = val.len() as u32;
        data_file.write_u32::<LittleEndian>(key_len)?;
        data_file.write_u32::<LittleEndian>(val_len)?;
        data_file.write_all(key)?;
        data_file.write_all(val)?;
        Ok(u64::from(8 + key_len + val_len))
    }

    pub fn id(&self) -> u128 {
        self.meta.metadata.id
    }

    pub fn merge_compact(tables: &Vec<SsTable>, level: u8, base_path: &str) -> io::Result<SsTable> {
        let size: u64 = tables.iter().map(|table| table.meta.size_bytes).sum();
        let mut iterators = Vec::with_capacity(tables.len());
        let mut values = Vec::with_capacity(tables.len());
        for table in tables.iter() {
            let mut iterator = table.into_iter();
            let value = iterator.next();
            iterators.push(iterator);
            values.push(value);
        }
        let metadata = SsTableMetadata::new(base_path.to_string(), level);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&metadata.data_path())?;
        let mut index: BTreeMap<ByteString, u64> = BTreeMap::new();
        let mut bloom_filter = BloomFilter::<ByteString>::new((size / 40) as usize, 0.01);
        let mut pos = 0u64;
        let mut counter = 0usize;
        loop {
            let mut current_idx: Option<usize> = None;
            for i in 0..iterators.len() {
                if let Some(kv) = values[i].as_ref() {
                    match current_idx {
                        None => {
                            current_idx = Some(i);
                        }
                        Some(curr) => {
                            if kv.key <= values[curr].as_ref().unwrap().key {
                                current_idx = Some(i);
                            }
                            if values[curr].as_ref().unwrap().key == kv.key {
                                values[curr] = iterators[curr].next()
                            }
                        }
                    }
                }
            }
            match current_idx {
                Some(idx) => {
                    let kv = values[idx].as_ref().unwrap();
                    if kv.value == vec![0] {
                        continue;
                    }
                    let diff = Self::write_key_value(&mut file, &kv.key, &kv.value)?;
                    if counter % INDEX_STEP == 0 {
                        index.insert(kv.key.clone(), pos);
                    }
                    counter += 1;
                    pos += diff;
                    bloom_filter.insert(&kv.key);
                    values[idx] = iterators[idx].next();
                }
                None => break
            }
        }
        SsTable::write_index(&metadata, &index)?;
        SsTable::write_checksums(&metadata)?;
        SsTable::write_bloom_filter(&metadata, &bloom_filter)?;
        SsTable::write_metadata(&metadata)?;
        let size = file.seek(SeekFrom::End(0))?;


        let mut queue = VecDeque::new();
        for _ in 0..8 {
            queue.push_back(DataFile::open(metadata.data_path().as_path())?);
        }
        Ok(SsTable {
            meta: SsTableMeta {
                metadata,
                index,
                bloom_filter,
                size_bytes: size,
            },
            data: Mutex::new(queue),
        })
    }
    pub fn close(&self) -> io::Result<()> {
        fs::remove_file(self.meta.metadata.metadata_path())?;
        fs::remove_file(self.meta.metadata.bloom_filter_path())?;
        fs::remove_file(self.meta.metadata.index_path())?;
        fs::remove_file(self.meta.metadata.checksum_path())?;
        fs::remove_file(self.meta.metadata.data_path())
    }
}

impl PartialEq<Self> for SsTable {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}

impl Eq for SsTable {}

impl PartialOrd for SsTable {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SsTable {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.eq(other) { Ordering::Equal } else if self.id() < other.id() {
            Ordering::Less
        } else {
            Ordering::Greater
        }
    }
}

impl<'a> IntoIterator for &'a SsTable {
    type Item = KeyValuePair;

    type IntoIter = Iter;

    fn into_iter(self) -> Self::IntoIter {
        let file = DataFile::open(self.meta.metadata.data_path().as_path()).unwrap();
        Iter {
            data: file,
            pos: 0,
        }
    }
}

pub struct Iter {
    data: DataFile,
    pos: u64,
}

impl Iterator for Iter {
    type Item = KeyValuePair;

    fn next(&mut self) -> Option<Self::Item> {
        match self.data.read_record(self.pos) {
            Ok(Some((kv_pair, len))) => {
                self.pos += len;
                Some(kv_pair)
            },
            Ok(None) => None,
            Err(err) => panic!("Unexpected error occurred. Err: {}", err)
        }
    }
}

impl Clone for SsTable {
    fn clone(&self) -> Self {
        SsTable::load(&self.meta.metadata.metadata_path())
            .expect("Can't load sstable file")
    }
}

impl DataFile {
    fn open(path: &Path) -> io::Result<DataFile> {
        let file = OpenOptions::new()
            .read(true)
            .open(path)?;
        Ok(DataFile {
            data: file,
        })
    }
    fn read_record(&mut self, pos: u64) -> io::Result<Option<(KeyValuePair, u64)>> {
        match self.read_record_unsafe(pos) {
            Ok(res) => Ok(Some(res)),
            Err(err) => match err.kind() {
                ErrorKind::UnexpectedEof => Ok(None),
                _ => Err(err)
            }
        }
    }

    fn read_record_unsafe(&mut self, pos: u64) -> io::Result<(KeyValuePair, u64)> {
        let seek_from = SeekFrom::Start(pos);
        self.data.seek(seek_from)?;
        let key_len = self.data.read_u32::<LittleEndian>()?;
        let val_len = self.data.read_u32::<LittleEndian>()?;
        let mut key: Vec<u8> = vec![0u8; key_len as usize];
        let mut val: Vec<u8> = vec![0u8; val_len as usize];
        self.data.read_exact(&mut key)?;
        self.data.read_exact(&mut val)?;
        Ok((KeyValuePair {
            key,
            value: val,
        }, u64::from(8 + key_len + val_len)))
    }

    fn scan_range(&mut self, key: &ByteStr, start: u64, end: u64) -> io::Result<Option<ByteString>> {
        let mut pos = start;
        while let Some((kv, len)) = self.read_record(pos)? {
            if kv.key == *key {
                return Ok(Some(kv.value));
            } else {
                pos += len;
                if pos >= end {
                    break;
                }
            }
        }
        Ok(None)
    }
}

impl SsTableMetadata {
    pub fn new(base_path: String, level: u8) -> SsTableMetadata {
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        let timestamp = since_the_epoch.as_millis();
        let metadata_filename = format!("metadata_{}.db", timestamp);
        let data_filename = format!("data_{}.db", timestamp);
        let index_filename = format!("index_{}.db", timestamp);
        let checksum_filename = format!("checksum_{}.db", timestamp);
        let bloom_filter_filename = format!("bloom_{}.db", timestamp);
        SsTableMetadata {
            base_path,
            level,
            id: timestamp,
            metadata_filename,
            data_filename,
            index_filename,
            checksum_filename,
            bloom_filter_filename,
        }
    }
    fn construct_path(&self, filename: &str) -> PathBuf {
        let mut path = PathBuf::from(&self.base_path);
        path.push(format!("level-{}", self.level));
        path.push(filename);
        path
    }

    pub(crate) fn data_path(&self) -> PathBuf {
        self.construct_path(&self.data_filename)
    }

    fn index_path(&self) -> PathBuf {
        self.construct_path(&self.index_filename)
    }

    fn checksum_path(&self) -> PathBuf {
        self.construct_path(&self.checksum_filename)
    }

    fn metadata_path(&self) -> PathBuf {
        self.construct_path(&self.metadata_filename)
    }

    fn bloom_filter_path(&self) -> PathBuf {
        self.construct_path(&self.bloom_filter_filename)
    }

    pub fn load(metadata_path: &Path) -> SsTableMetadata {
        let metadata_file = OpenOptions::new()
            .read(true)
            .open(metadata_path)
            .expect("Can't open metadata file");
        serde_json::from_reader(metadata_file)
            .expect("Can't read metadata file, file with unknown format")
    }
}
#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::{env, fs, io};
    use rand::Rng;
    use crate::config::Config;
    use crate::Db;

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
