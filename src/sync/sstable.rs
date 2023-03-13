extern crate probabilistic_collections;

use std::{fs, io};
use std::cmp::Ordering;
use std::io::Write;
use std::path::Path;


use crate::{ByteStr, KeyValuePair};
use crate::checksums::Checksums;
use crate::datafile::{ReadOnlyDataFile, SizedFile, WriteableDataFile};
use crate::memtable::{ByteString, MemTable};
use crate::sstable_bloom_filter::SstableBloomFilter;
use crate::sstable_metadata::SsTableMetadata;
use crate::sstable_index::SstableIndex;

const INDEX_STEP: usize = 100;


pub struct SsTable {
    metadata: SsTableMetadata,
    data: ReadOnlyDataFile,
    index: SstableIndex,
    bloom_filter: SstableBloomFilter,
    size_bytes: u64,
}

impl Clone for SsTable {
    fn clone(&self) -> Self {
        let metadata = self.metadata.clone();
        SsTable::load(&metadata.metadata_path())
            .expect("Can't load sstable file")
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

impl<'a> IntoIterator for &'a mut SsTable {
    type Item = KeyValuePair;

    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        Iter {
            table: self,
            pos: 0,
        }
    }
}

pub struct Iter<'a> {
    table: &'a mut SsTable,
    pos: u64,
}

impl<'a> Iterator for Iter<'a> {
    type Item = KeyValuePair;

    fn next(&mut self) -> Option<Self::Item> {
        match self.table.data.read_record(self.pos) {
            Ok(Some((kv_pair, len))) => {
                self.pos += len;
                Some(kv_pair)
            },
            Ok(None) => None,
            Err(err) => panic!("Unexpected error occurred. Err: {}", err)
        }
    }
}

impl SsTable {
    pub fn id(&self) -> u128 {
        self.metadata.id
    }

    pub fn get(&mut self, key: &ByteStr) -> io::Result<Option<ByteString>> {
        if !self.bloom_filter.contains(key) {
            return Ok(None);
        }

        let res = match self.index.get(key) {
            Some(pos) => {
                let position = *pos;
                self.data.read_record(position)
                    .map(|op| op.map(|p| p.0))
            }
            None => {
                let (start, end) = self.index.position_range(key, self.size_bytes);
                self.data.scan_range(key, start, end)
            }
        }?;
        Ok(res.map(|kv| kv.value_owned()))
    }

}


impl SsTable {
    pub fn load(metadata_path: &Path) -> io::Result<SsTable> {
        let metadata = SsTableMetadata::load(metadata_path);
        Checksums::verify(&metadata)?;
        let mut data_file = ReadOnlyDataFile::open(&metadata.data_path())
            .expect("Can't create/open data file");
        let index = SstableIndex::load(&metadata.index_path())
            .expect("Can't open index file");
        let bloom_filter = SstableBloomFilter::load(&metadata.bloom_filter_path())
            .expect("Cant open bloom filter file");
        let size = data_file.size()?;
        Ok(SsTable {
            metadata,
            data: data_file,
            index,
            bloom_filter,
            size_bytes: size,
        })
    }

    pub fn from_memtable(base_path: &str, memtable: &MemTable) -> io::Result<SsTable> {
        let metadata = SsTableMetadata::new(base_path.to_string(), 0);
        let (data_file, index, bloom_filter, size) =
            SsTable::write_data_file(&metadata, memtable)?;
        index.write_to_file(&metadata.index_path())?;
        Checksums::write_checksums(&metadata)?;
        bloom_filter.write_to_file(&metadata.bloom_filter_path())?;
        metadata.write_to_file()?;
        Ok(SsTable {
            metadata,
            data: data_file,
            index,
            bloom_filter,
            size_bytes: size,
        })
    }

    pub fn merge_compact(tables: &mut Vec<SsTable>, level: u8, base_path: &str) -> io::Result<SsTable> {
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
        let mut file = WriteableDataFile::open(&metadata.data_path())?;
        let mut index = SstableIndex::new();
        let mut bloom_filter = SstableBloomFilter::new((size / 40) as usize);
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
                            if kv.key_ref() <= values[curr].as_ref().unwrap().key_ref() {
                                current_idx = Some(i);
                            }
                            if values[curr].as_ref().unwrap().key_ref() == kv.key_ref() {
                                values[curr] = iterators[curr].next()
                            }
                        }
                    }
                }
            }
            match current_idx {
                Some(idx) => {
                    let kv = values[idx].as_ref().unwrap();
                    if kv.value_ref() == vec![0] {
                        continue;
                    }
                    let diff = file.write_key_value(kv.key_ref(), kv.value_ref())?;

                    bloom_filter.insert(kv.key_ref());
                    if counter % INDEX_STEP == 0 {
                        index.insert(kv.key_cloned(), pos);
                    }
                    counter += 1;
                    pos += diff;
                    values[idx] = iterators[idx].next();
                }
                None => break
            }
        }
        index.write_to_file(&metadata.index_path())?;
        Checksums::write_checksums(&metadata)?;
        bloom_filter.write_to_file(&metadata.bloom_filter_path())?;
        metadata.write_to_file()?;
        let size = file.size()?;

        let data_file = ReadOnlyDataFile::open(&metadata.data_path())?;

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

    fn write_data_file(metadata: &SsTableMetadata, memtable: &MemTable) -> io::Result<(ReadOnlyDataFile, SstableIndex, SstableBloomFilter, u64)> {
        let mut data_file = WriteableDataFile::open(&metadata.data_path())?;
        let mut index = SstableIndex::new();
        let mut bloom_filter = SstableBloomFilter::new(memtable.size());
        let mut pos = 0;
        for (i, (key, val)) in memtable.into_iter().enumerate() {
            let diff = data_file.write_key_value(key, val)?;
            bloom_filter.insert(key);
            if i % INDEX_STEP == 0 {
                index.insert(key.clone(), pos);
            }
            pos += diff;
        }
        data_file.flush()?;

        let data_file = ReadOnlyDataFile::open(&metadata.data_path())?;
        Ok((data_file, index, bloom_filter, pos))
    }


}

#[cfg(test)]
mod tests {
    use std::{env, fs};

    use serial_test::serial;

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
            assert_eq!(entries[i].0, kv.key_ref());
            assert_eq!(entries[i].1, kv.value_ref());
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

    fn check_values(sstable: &mut SsTable) {
        for i in 0..500 {
            let val = sstable.get(&i.to_string().into_bytes()).unwrap();
            assert_eq!(true, val.is_some());
            assert_eq!((i * 100).to_string().into_bytes(), val.unwrap());
        }
    }
}
