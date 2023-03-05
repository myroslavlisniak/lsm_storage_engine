extern crate probabilistic_collections;

use std::{fs, io};
use std::cmp::Ordering;
use std::collections::btree_map::{BTreeMap, Range};
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::Path;

use base64::encode as base64_encode;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use log::debug;
use probabilistic_collections::bloom::BloomFilter;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use crate::{ByteStr, KeyValuePair};
use crate::datafile::DataFile;

use crate::memtable::{ByteString, MemTable};
use crate::sstable_metadata::SsTableMetadata;

const INDEX_STEP: usize = 100;

pub type SstableIndex = BTreeMap<ByteString, u64>;
pub type SstableBloomFilter = BloomFilter<ByteString>;

#[derive(Serialize, Deserialize)]
struct Checksums {
    index_checksum: String,
    data_checksum: String,
}



pub struct SsTable<T: Seek + Read> {
    metadata: SsTableMetadata,
    data: T,
    index: SstableIndex,
    bloom_filter: SstableBloomFilter,
    size_bytes: u64,
}

impl Clone for SsTable<DataFile> {
    fn clone(&self) -> Self {
        let metadata = self.metadata.clone();
        SsTable::load(&metadata.metadata_path())
            .expect("Can't load sstable file")
    }
}

impl<T: Seek + Read> PartialEq<Self> for SsTable<T> {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}

impl<T: Seek + Read> Eq for SsTable<T> {}

impl<T: Seek + Read> PartialOrd for SsTable<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Seek + Read> Ord for SsTable<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.eq(other) { Ordering::Equal } else if self.id() < other.id() {
            Ordering::Less
        } else {
            Ordering::Greater
        }
    }
}

impl<'a, T: Seek + Read> IntoIterator for &'a mut SsTable<T> {
    type Item = KeyValuePair;

    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        Iter {
            table: self,
            pos: 0,
        }
    }
}

pub struct Iter<'a, T: Seek + Read> {
    table: &'a mut SsTable<T>,
    pos: u64,
}

impl<'a, T: Seek + Read> Iterator for Iter<'a, T> {
    type Item = KeyValuePair;

    fn next(&mut self) -> Option<Self::Item> {
        match self.table.read_record(self.pos) {
            Ok(Some((kv_pair, len))) => {
                self.pos += len;
                Some(kv_pair)
            },
            Ok(None) => None,
            Err(err) => panic!("Unexpected error occurred. Err: {}", err)
        }
    }
}

impl<T: Seek + Read> SsTable<T> {
    pub fn id(&self) -> u128 {
        self.metadata.id
    }

    pub fn get(&mut self, key: &ByteStr) -> io::Result<Option<ByteString>> {
        if !self.bloom_filter.contains(key) {
            return Ok(None);
        }
        match self.index.get(key) {
            Some(pos) => {
                let position = *pos;
                self.read_record(position)
                    .map(|op| op.map(|p| p.0.value))
            }
            None => {
                let (start, end) = self.position_range(key);
                self.scan_range(key, start, end)
            }
        }
    }

    fn position_range(&self, key: &ByteStr) -> (u64, u64) {
        let mut range: Range<ByteString, u64> = self.index.range::<ByteString, _>(..key.to_vec());
        let start = range.next_back().map(|r| *r.1).unwrap_or(0);
        let mut range: Range<ByteString, u64> = self.index.range::<ByteString, _>(key.to_vec()..);
        let end = range.next().map(|e| *e.1).unwrap_or(self.size_bytes);
        (start, end)
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
}


impl SsTable<DataFile> {
    pub fn load(metadata_path: &Path) -> io::Result<SsTable<DataFile>> {
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
        let mut data_file = DataFile::open(&metadata.data_path())
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
        Ok(SsTable {
            metadata,
            data: data_file,
            index,
            bloom_filter,
            size_bytes: size,
        })
    }

    pub fn from_memtable(base_path: &str, memtable: &MemTable) -> io::Result<SsTable<DataFile>> {
        let metadata = SsTableMetadata::new(base_path.to_string(), 0);
        let (data_file, index, bloom_filter, size) =
            SsTable::write_data_file(&metadata, memtable)?;
        SsTable::write_index(&metadata, &index)?;
        SsTable::write_checksums(&metadata)?;
        SsTable::write_bloom_filter(&metadata, &bloom_filter)?;
        SsTable::write_metadata(&metadata)?;
        Ok(SsTable {
            metadata,
            data: data_file,
            index,
            bloom_filter,
            size_bytes: size,
        })
    }

    pub fn merge_compact(tables: &mut Vec<SsTable<DataFile>>, level: u8, base_path: &str) -> io::Result<SsTable<DataFile>> {
        let size: u64 = tables.iter().map(|table| table.size_bytes).sum();
        let mut iterators = Vec::with_capacity(tables.len());
        let mut values = Vec::with_capacity(tables.len());
        for table in tables.iter_mut() {
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

        let data_file = DataFile::open(&metadata.data_path())?;

        Ok(SsTable {
            metadata,
            data: data_file,
            index,
            bloom_filter,
            size_bytes: size,
        })
    }

    pub fn close(&self) -> io::Result<()> {
        fs::remove_file(self.metadata.metadata_path())?;
        fs::remove_file(self.metadata.bloom_filter_path())?;
        fs::remove_file(self.metadata.index_path())?;
        fs::remove_file(self.metadata.checksum_path())?;
        fs::remove_file(self.metadata.data_path())
    }

    fn write_data_file(metadata: &SsTableMetadata, memtable: &MemTable) -> io::Result<(DataFile, SstableIndex, SstableBloomFilter, u64)> {
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

        let data_file = DataFile::open(&metadata.data_path())?;
        Ok((data_file, index, bloom_filter, pos))
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

}

#[cfg(test)]
mod tests {
    use std::{env, fs};
    use std::fs::File;
    use serial_test::serial;
    use crate::datafile::DataFile;

    use crate::memtable::MemTable;
    use crate::sync::sstable::SsTable;

    fn prepare_directories() -> String{
        let mut buf = env::temp_dir();
        buf.push("sstable_test");
        let base_dir = buf.to_str().expect("Can't get temp directory");
        fs::remove_dir_all(base_dir).unwrap_or(());
        fs::create_dir_all(base_dir).unwrap();
        for i in 0..5 {
            let mut path_buf = buf.clone();
            path_buf.push(format!("level-{}", i));
            fs::create_dir_all(&path_buf).unwrap();
        }
        base_dir.to_string()
    }

    #[test]
    #[serial]
    fn sstable_test() {
        let base_dir = prepare_directories();
        let mut memtable = MemTable::new_in_memory_log();
        for i in 0..500 {
            let val = i * 100;
            memtable.insert(i.to_string().into_bytes(), val.to_string().into_bytes());
        }
        let mut sstable = SsTable::from_memtable(&base_dir, &memtable).unwrap();
        check_values(&mut sstable);
        assert_eq!(None, sstable.get(&"1000".to_string().into_bytes()).unwrap());
    }

    #[test]
    #[serial]
    fn sstable_iterator_test() {
        let base_dir = prepare_directories();
        let mut memtable = MemTable::new_in_memory_log();
        let mut entries = Vec::new();
        for i in 1..500 {
            let val = i * 100;
            let key = i.to_string().into_bytes();
            let val = val.to_string().into_bytes();
            entries.push((key.clone(), val.clone()));
            memtable.insert(key, val);
        }
        let mut sstable = SsTable::from_memtable(&base_dir, &memtable).unwrap();
        let mut i = 0;
        entries.sort_by(|a,b| a.0.cmp(&b.0));
        for kv in sstable.into_iter() {
            assert_eq!(entries[i].0, kv.key);
            assert_eq!(entries[i].1, kv.value);
            i += 1;
        }
    }

    #[test]
    #[serial]
    fn sstable_load_from_file_test() {
        let base_dir = prepare_directories();
        let mut memtable = MemTable::new_in_memory_log();
        for i in 0..500 {
            let val = i * 100;
            memtable.insert(i.to_string().into_bytes(), val.to_string().into_bytes());
        }
        let sstable = SsTable::from_memtable(&base_dir, &memtable).unwrap();
        let mut sstable = SsTable::load(&sstable.metadata.metadata_path()).unwrap();
        check_values(&mut sstable)
    }

    fn check_values(sstable: &mut SsTable<DataFile>) {
        for i in 0..500 {
            let val = sstable.get(&i.to_string().into_bytes()).unwrap();
            assert_eq!(true, val.is_some());
            assert_eq!((i * 100).to_string().into_bytes(), val.unwrap());
        }
    }
}
