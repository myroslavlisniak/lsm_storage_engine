use std::collections::{BTreeMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::{fs, io, mem};
use std::cmp::Ordering;
use std::collections::btree_map::Range;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::Path;
use log::debug;
use parking_lot::Mutex;
use probabilistic_collections::bloom::BloomFilter;
use sha2::{Digest, Sha256};
use serde::{Serialize, Deserialize};
use base64::encode as base64_encode;
use crate::{ByteStr, ByteString, KeyValuePair};
use crate::datafile::DataFile;
use crate::memtable::MemTable;
use crate::sstable_metadata::SsTableMetadata;

const INDEX_STEP: usize = 100;

pub type SstableIndex = BTreeMap<ByteString, u64>;
pub type SstableBloomFilter = BloomFilter<ByteString>;

#[derive(Serialize, Deserialize)]
struct Checksums {
    index_checksum: String,
    data_checksum: String,
}


struct SsTableMeta {
    metadata: SsTableMetadata,
    index: SstableIndex,
    bloom_filter: SstableBloomFilter,
    size_bytes: u64,
}



pub(crate) struct SsTable {
    meta: SsTableMeta,
    data: Mutex<VecDeque<DataFile>>,
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
            let diff = DataFile::write_key_value(&mut data_file, key, val)?;
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
        Ok(base64_encode(hash))
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
                    let diff = DataFile::write_key_value(&mut file, &kv.key, &kv.value)?;
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





