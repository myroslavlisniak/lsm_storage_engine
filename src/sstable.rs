use std::io;
use std::collections::BTreeMap;
use std::collections::btree_map::Range;
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::encode as base64_encode;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::memtable::{ByteString, MemTable};
use crate::segment::KeyValuePair;
extern crate probabilistic_collections;
use probabilistic_collections::bloom::BloomFilter;

#[derive(Serialize, Deserialize)]
struct Checksums {
    index_checksum: String,
    data_checksum: String,
}

#[derive(Serialize, Deserialize)]
pub struct SSTableMetadata {
    base_path: String,
    level: u8,
    metadata_filename: String,
    checksum_filename: String,
    data_filename: String,
    index_filename: String,
    bloom_filter_filename: String,
}


impl SSTableMetadata {
    fn new(base_path: String) -> SSTableMetadata {
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        let timestamp = since_the_epoch.as_secs();
        let metadata_filename = format!("metadata_{}.db", timestamp);
        let data_filename = format!("data_{}.db", timestamp);
        let index_filename = format!("index_{}.db", timestamp);
        let checksum_filename = format!("checksum_{}.db", timestamp);
        let bloom_filter_filename = format!("bloom_{}.db", timestamp);
        SSTableMetadata {
            base_path,
            level: 0,
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

    fn data_path(&self) -> PathBuf {
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

    fn load(metadata_path: &Path) -> io::Result<SSTableMetadata> {
        let metadata_file = OpenOptions::new()
            .read(true)
            .open(metadata_path)
            .expect("Can't open metadata file");
        serde_json::from_reader(metadata_file)
            .map_err(|e| io::Error::from(e))
    }
}

pub struct SSTable<T: Seek + Read> {
    metadata: SSTableMetadata,
    data: T,
    index: BTreeMap<ByteString, u64>,
    bloom_filter: BloomFilter<ByteString>
}

impl<T: Seek + Read> SSTable<T> {
    pub fn get(&mut self, key: &ByteString) -> io::Result<Option<ByteString>> {
        if !self.bloom_filter.contains(key) {
            return Ok(None);
        }
        match self.index.get(key) {
            Some(pos) => {
                let (record, _) = self.read_record(*pos)?;
                return Ok(Some(record.value));
            }
            None => {
                let mut range: Range<ByteString, u64> = self.index.range::<ByteString, _>(..key);
                if let Some((_, start)) = range.next_back() {
                    let mut pos = *start;
                    let mut range: Range<ByteString, u64> = self.index.range::<ByteString, _>(key..);
                    let end = range.next().map(|e| *e.1);
                    loop {
                        match self.read_record(pos) {
                            Ok((kv, len)) => if kv.key == *key {
                                return Ok(Some(kv.value));
                            } else {
                                pos += len;
                            },
                            Err(err) =>
                                match err.kind() {
                                    io::ErrorKind::UnexpectedEof => {
                                        break;
                                    }
                                    _ => return Err(err)
                                }
                        }
                        match end {
                            Some(e) => if pos == e { break; }
                            None => ()//do nothing
                        }
                    }
                }
                Ok(None)
            }
        }
    }

    fn read_record(&mut self, pos: u64) -> io::Result<(KeyValuePair, u64)> {
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

impl SSTable<File> {
    fn load(metadata_path: &Path) -> io::Result<SSTable<File>> {
        let metadata = SSTableMetadata::load(metadata_path)?;
        let calculated_data_hash = SSTable::calculate_checksum(&metadata.data_path())?;
        let calculated_index_hash = SSTable::calculate_checksum(&metadata.index_path())?;
        let checksum_file = OpenOptions::new()
            .read(true)
            .open(&metadata.checksum_path())
            .expect("Can't open checksum file");
        let checksums: Checksums = serde_json::from_reader(checksum_file).map_err(|e| io::Error::from(e))?;
        if calculated_data_hash != checksums.data_checksum {
            panic!("Can't load SSTable from {}. Checksum is not correct", &metadata.data_filename);
        }
        if calculated_index_hash != checksums.index_checksum {
            panic!("Can't load SSTable from {}. Checksum is not correct", &metadata.index_filename);
        }
        let data_file = OpenOptions::new()
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
        Ok(SSTable {
            metadata,
            data: data_file,
            index,
            bloom_filter
        })
    }

    fn from_memtable<Log: Read + Write>(base_path: &str, memtable: &MemTable<Log>) -> io::Result<SSTable<File>> {
        let metadata = SSTableMetadata::new(base_path.to_string());
        for i in 0..5 {
            let mut path_buf = PathBuf::from(base_path);
            path_buf.push(format!("level-{}", i));
            std::fs::create_dir_all(&path_buf);
        }
        let (data_file, index, bloom_filter) = SSTable::write_data_file(&metadata, memtable)?;
        SSTable::write_index(&metadata, &index)?;
        SSTable::write_checksums(&metadata)?;
        SSTable::write_bloom_filter(&metadata, &bloom_filter)?;
        SSTable::write_metadata(&metadata)?;
        Ok(SSTable {
            metadata,
            data: data_file,
            index,
            bloom_filter
        })
    }
    fn write_data_file<Log: Read + Write>(metadata: &SSTableMetadata, memtable: &MemTable<Log>) -> io::Result<(File, BTreeMap<ByteString, u64>, BloomFilter<ByteString>)> {
        let mut data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&metadata.data_path())
            .expect("Can't create/open data file");
        let mut index: BTreeMap<ByteString, u64> = BTreeMap::new();
        let mut bloom_filter = BloomFilter::<ByteString>::new(memtable.size(), 0.01);
        let mut pos = 0;
        for (i, (key, val)) in memtable.into_iter().enumerate() {
            let key_len = key.len() as u32;
            let val_len = val.len() as u32;
            data_file.write_u32::<LittleEndian>(key_len)?;
            data_file.write_u32::<LittleEndian>(val_len)?;
            data_file.write_all(&key)?;
            data_file.write_all(&val)?;
            bloom_filter.insert(key);
            if i % 100 == 0 {
                index.insert(key.clone(), pos);
            }
            pos += u64::from(8 + key_len + val_len);
        }
        data_file.flush()?;
        Ok((data_file, index, bloom_filter))
    }

    fn write_checksums(metadata: &SSTableMetadata) -> io::Result<()> {
        let data_base64_hash = SSTable::calculate_checksum(&metadata.data_path())?;
        let index_base64_hash = SSTable::calculate_checksum(&metadata.index_path())?;
        println!("Base64-encoded hash: {}", data_base64_hash);
        let checksums = Checksums {
            index_checksum: index_base64_hash,
            data_checksum: data_base64_hash,
        };
        let checksum_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&metadata.checksum_path())?;
        serde_json::to_writer(checksum_file, &checksums)
            .map_err(|e| io::Error::from(e))
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

    fn write_index(metadata: &SSTableMetadata, index: &BTreeMap<ByteString, u64>) -> io::Result<()> {
        let index_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&metadata.index_path())?;
        bincode::serialize_into(index_file, &index)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))
    }
    fn write_metadata(metadata: &SSTableMetadata) -> io::Result<()> {
        let metadata_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&metadata.metadata_path())?;
        serde_json::to_writer(metadata_file, metadata)
            .map_err(|e| io::Error::from(e))
    }

    fn write_bloom_filter(metadata: &SSTableMetadata, bloom_filter: &BloomFilter<ByteString>) -> io::Result<()> {
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
    use tokio::io;
    use crate::memtable::MemTable;
    use crate::sstable::SSTable;

    #[test]
    fn sstable_test() -> io::Result<()>{
        let mut memtable = MemTable::new_in_memory_log();
        for i in 0..500 {
            let val = i * 100;
            memtable.insert(i.to_string().into_bytes(), val.to_string().into_bytes());
        }
        let mut buf = env::temp_dir();
        buf.push("sstable_test");
        let base_dir = buf.to_str().expect("Can't get temp directory");
        fs::create_dir_all(base_dir)?;
        let mut sstable = SSTable::from_memtable(base_dir, &memtable)?;
        for i in 0..500 {
            let val = sstable.get(&i.to_string().into_bytes())?;
            assert_eq!(true, val.is_some());
            assert_eq!((i*100).to_string().into_bytes(), val.unwrap());
        }
        assert_eq!(None, sstable.get(&"1000".to_string().into_bytes())?);
        Ok(())
    }

    #[test]
    fn sstable_load_from_file_test() -> io::Result<()>{
        let mut memtable = MemTable::new_in_memory_log();
        for i in 0..500 {
            let val = i * 100;
            memtable.insert(i.to_string().into_bytes(), val.to_string().into_bytes());
        }
        let mut buf = env::temp_dir();
        buf.push("sstable_from_file_test");
        let base_dir = buf.to_str().expect("Can't get temp directory");
        // fs::remove_dir_all(base_dir)?;
        fs::create_dir_all(base_dir)?;

        let mut sstable = SSTable::from_memtable(base_dir, &memtable)?;
        let mut sstable = SSTable::load(&sstable.metadata.metadata_path())?;
        for i in 0..500 {
            let val = sstable.get(&i.to_string().into_bytes())?;
            assert_eq!(true, val.is_some());
            assert_eq!((i*100).to_string().into_bytes(), val.unwrap());
        }

        Ok(())
    }
}