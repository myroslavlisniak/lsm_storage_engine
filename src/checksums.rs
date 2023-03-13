use std::fs::OpenOptions;
use std::io;
use std::io::Read;
use std::path::Path;

use base64::encode as base64_encode;
use log::debug;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::sstable_metadata::SsTableMetadata;

#[derive(Serialize, Deserialize)]
pub(crate) struct Checksums {
    index_checksum: String,
    data_checksum: String,
}

impl Checksums {
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

    pub(crate) fn verify(metadata: &SsTableMetadata) -> io::Result<()> {
        let calculated_data_hash = Checksums::calculate_checksum(&metadata.data_path())?;
        let calculated_index_hash = Checksums::calculate_checksum(&metadata.index_path())?;
        let checksum_file = OpenOptions::new()
            .read(true)
            .open(&metadata.checksum_path())
            .expect("Can't open checksum file");
        let checksums: Checksums =
            serde_json::from_reader(checksum_file).map_err(io::Error::from)?;
        if calculated_data_hash != checksums.data_checksum {
            panic!(
                "Can't load SSTable from {}. Checksum is not correct",
                &metadata.data_filename
            );
        }
        if calculated_index_hash != checksums.index_checksum {
            panic!(
                "Can't load SSTable from {}. Checksum is not correct",
                &metadata.index_filename
            );
        }
        Ok(())
    }

    pub(crate) fn write_checksums(metadata: &SsTableMetadata) -> io::Result<()> {
        let data_base64_hash = Self::calculate_checksum(&metadata.data_path())?;
        let index_base64_hash = Self::calculate_checksum(&metadata.index_path())?;
        debug!(
            "Base64-encoded hash: {}, for file: {}",
            data_base64_hash, &metadata.data_filename
        );
        let checksums = Checksums {
            index_checksum: index_base64_hash,
            data_checksum: data_base64_hash,
        };
        let checksum_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&metadata.checksum_path())?;
        serde_json::to_writer(checksum_file, &checksums).map_err(io::Error::from)
    }
}
