use crate::{ByteStr, ByteString};
use std::collections::btree_map::Range;
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::ErrorKind;
use std::path::Path;
use tokio::io;

pub(crate) struct SstableIndex {
    map: BTreeMap<ByteString, u64>,
}

impl SstableIndex {
    pub(crate) fn new() -> SstableIndex {
        SstableIndex {
            map: BTreeMap::new(),
        }
    }

    pub(crate) fn load(path: &Path) -> io::Result<SstableIndex> {
        let index_file = OpenOptions::new().read(true).open(path)?;
        let index: BTreeMap<ByteString, u64> = bincode::deserialize_from(index_file)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;
        Ok(SstableIndex { map: index })
    }
    pub(crate) fn get(&self, key: &ByteStr) -> Option<&u64> {
        self.map.get(key)
    }

    pub(crate) fn insert(&mut self, key: ByteString, val: u64) {
        self.map.insert(key, val);
    }

    pub(crate) fn position_range(&self, key: &ByteStr, size_bytes: u64) -> (u64, u64) {
        let mut range: Range<ByteString, u64> = self.map.range::<ByteString, _>(..key.to_vec());
        let start = range.next_back().map(|r| *r.1).unwrap_or(0);
        let mut range: Range<ByteString, u64> = self.map.range::<ByteString, _>(key.to_vec()..);
        let end = range.next().map(|e| *e.1).unwrap_or(size_bytes);
        (start, end)
    }

    pub(crate) fn write_to_file(&self, path: &Path) -> io::Result<()> {
        let index_file = OpenOptions::new().write(true).create(true).open(path)?;
        bincode::serialize_into(index_file, &self.map)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))
    }
}
