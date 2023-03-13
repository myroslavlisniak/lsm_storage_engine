use crate::{ByteStr, ByteString};
use probabilistic_collections::bloom::BloomFilter;
use std::fs::OpenOptions;
use std::io;
use std::io::ErrorKind;
use std::path::Path;

pub(crate) struct SstableBloomFilter {
    bloom_filter: BloomFilter<ByteString>,
}

impl SstableBloomFilter {
    pub(crate) fn new(size: usize) -> SstableBloomFilter {
        SstableBloomFilter {
            bloom_filter: BloomFilter::new(size, 0.01),
        }
    }

    pub(crate) fn load(path: &Path) -> io::Result<SstableBloomFilter> {
        let bloom_filter_file = OpenOptions::new().read(true).open(path)?;
        let bloom_filter = bincode::deserialize_from(bloom_filter_file)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;
        Ok(SstableBloomFilter { bloom_filter })
    }

    pub(crate) fn contains(&self, key: &ByteStr) -> bool {
        self.bloom_filter.contains(key)
    }

    pub(crate) fn insert(&mut self, key: &ByteStr) {
        self.bloom_filter.insert(key);
    }

    pub(crate) fn write_to_file(&self, path: &Path) -> io::Result<()> {
        let bloom_filter_file = OpenOptions::new().write(true).create(true).open(path)?;
        bincode::serialize_into(bloom_filter_file, &self.bloom_filter)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))
    }
}
