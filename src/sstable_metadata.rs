use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct SsTableMetadata {
    pub(crate) base_path: String,
    pub(crate) id: u128,
    pub(crate) level: u8,
    pub(crate) metadata_filename: String,
    pub(crate) checksum_filename: String,
    pub(crate) data_filename: String,
    pub(crate) index_filename: String,
    pub(crate) bloom_filter_filename: String,
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

    pub(crate) fn index_path(&self) -> PathBuf {
        self.construct_path(&self.index_filename)
    }

    pub(crate) fn checksum_path(&self) -> PathBuf {
        self.construct_path(&self.checksum_filename)
    }

    pub(crate) fn metadata_path(&self) -> PathBuf {
        self.construct_path(&self.metadata_filename)
    }

    pub(crate) fn bloom_filter_path(&self) -> PathBuf {
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

    pub(crate) fn write_to_file(&self) -> io::Result<()> {
        let metadata_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&self.metadata_path())?;
        serde_json::to_writer(metadata_file, self)
            .map_err(io::Error::from)
    }
}
