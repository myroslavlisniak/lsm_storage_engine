use std::cmp::Ordering;
use std::collections::VecDeque;
use std::io::Write;
use std::path::Path;
use std::{fs, io, mem};

use parking_lot::Mutex;

use crate::checksums::Checksums;
use crate::datafile::{ReadOnlyDataFile, SizedFile, WriteableDataFile};
use crate::memtable::MemTable;
use crate::sstable_bloom_filter::SstableBloomFilter;
use crate::sstable_index::SstableIndex;
use crate::sstable_metadata::SsTableMetadata;
use crate::{ByteStr, ByteString, KeyValuePair};

const INDEX_STEP: usize = 100;

struct SsTableMeta {
    metadata: SsTableMetadata,
    index: SstableIndex,
    bloom_filter: SstableBloomFilter,
    size_bytes: u64,
}

pub(crate) struct SsTable {
    meta: SsTableMeta,
    data: Mutex<VecDeque<ReadOnlyDataFile>>,
}

impl SsTable {
    pub fn load(metadata_path: &Path) -> io::Result<SsTable> {
        let metadata = SsTableMetadata::load(metadata_path);
        Checksums::verify(&metadata)?;
        let mut data_file =
            ReadOnlyDataFile::open(&metadata.data_path()).expect("Can't create/open data file");
        let index = SstableIndex::load(&metadata.index_path()).expect("Can't open index file");
        let bloom_filter = SstableBloomFilter::load(&metadata.bloom_filter_path())
            .expect("Can't open bloom filter file");
        let size = data_file.size()?;
        let mut queue = VecDeque::new();
        //todo config
        for _ in 0..8 {
            queue.push_back(ReadOnlyDataFile::open(metadata.data_path().as_path()).unwrap());
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
                data.read_record(position).map(|op| op.map(|p| p.0))
            }
            None => {
                let (start, end) = self.meta.index.position_range(key, self.meta.size_bytes);
                data.scan_range(key, start, end)
            }
        }?;
        {
            self.data.lock().push_back(data);
        }
        Ok(result.map(|kv| kv.value_owned()))
    }

    pub fn from_memtable(base_path: &str, memtable: &MemTable) -> io::Result<SsTable> {
        let metadata = SsTableMetadata::new(base_path.to_string(), 0);
        let (_, index, bloom_filter, size) = SsTable::write_data_file(&metadata, memtable)?;
        index.write_to_file(&metadata.index_path())?;
        Checksums::write_checksums(&metadata)?;
        bloom_filter.write_to_file(&metadata.bloom_filter_path())?;
        metadata.write_to_file()?;
        let mut queue = VecDeque::new();
        //todo config
        for _ in 0..8 {
            queue.push_back(ReadOnlyDataFile::open(metadata.data_path().as_path()).unwrap());
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

    fn write_data_file(
        metadata: &SsTableMetadata,
        memtable: &MemTable,
    ) -> io::Result<(ReadOnlyDataFile, SstableIndex, SstableBloomFilter, u64)> {
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
                    if counter % INDEX_STEP == 0 {
                        index.insert(kv.key_cloned(), pos);
                    }
                    counter += 1;
                    pos += diff;
                    bloom_filter.insert(kv.key_ref());
                    values[idx] = iterators[idx].next();
                }
                None => break,
            }
        }
        index.write_to_file(&metadata.index_path())?;
        Checksums::write_checksums(&metadata)?;
        bloom_filter.write_to_file(&metadata.bloom_filter_path())?;
        metadata.write_to_file()?;
        let size = file.size()?;

        let mut queue = VecDeque::new();
        for _ in 0..8 {
            queue.push_back(ReadOnlyDataFile::open(metadata.data_path().as_path())?);
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
        if self.eq(other) {
            Ordering::Equal
        } else if self.id() < other.id() {
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
        let file = ReadOnlyDataFile::open(self.meta.metadata.data_path().as_path()).unwrap();
        Iter { data: file, pos: 0 }
    }
}

pub struct Iter {
    data: ReadOnlyDataFile,
    pos: u64,
}

impl Iterator for Iter {
    type Item = KeyValuePair;

    fn next(&mut self) -> Option<Self::Item> {
        match self.data.read_record(self.pos) {
            Ok(Some((kv_pair, len))) => {
                self.pos += len;
                Some(kv_pair)
            }
            Ok(None) => None,
            Err(err) => panic!("Unexpected error occurred. Err: {}", err),
        }
    }
}

impl Clone for SsTable {
    fn clone(&self) -> Self {
        SsTable::load(&self.meta.metadata.metadata_path()).expect("Can't load sstable file")
    }
}
