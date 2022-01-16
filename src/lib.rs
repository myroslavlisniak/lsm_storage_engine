extern crate byteorder;
extern crate config as config_lib;
extern crate crc;
extern crate serde_derive;

use std::convert::TryInto;
use std::fs;
use std::fs::DirEntry;
use std::io;
use std::io::Error;
use std::path::{Path, PathBuf};

use itertools::Itertools;
use log::{debug, error, info};

use crate::config::Config;
use crate::segment::ByteStr;
use crate::segment::ByteString;
use crate::segment::Segment;
use crate::segment::SegmentError;

pub mod segment;
pub mod command;
pub mod config;
mod memtable;
mod wal;

const FILE_SIZE_LIMIT: u64 = 200;

#[derive(Debug)]
pub struct Storage {
    segments: Vec<Segment>,
    current_index: usize,
    config: Config,
}

impl Storage {
    pub fn open(config: Config) -> io::Result<Self> {
        info!("Creating storage using folder: {}", &config.base_path);
        let path = std::path::Path::new(&config.base_path);
        if !path.is_dir() {
            error!("{:?} is not directory", path.to_str());
            return Err(Error::new(io::ErrorKind::NotFound, "should be directory"));
        }
        let mut tmp_path = path.to_path_buf();
        tmp_path.push("tmp");
        fs::create_dir_all(tmp_path).unwrap();
        let mut segments: Vec<Segment> = Vec::new();
        let files: Vec<DirEntry> = fs::read_dir(path)?
            .map(|r| r.unwrap())
            .filter(|r| r.file_name() != "tmp")
            .collect();
        if files.is_empty() {
            debug!("Data folder is empty. Creating first segment file");
            let segment = Segment::new(0, path).expect("Can't create new segment file");
            segments.push(segment);
        } else {
            files.into_iter()
                .map(|dir_entry| dir_entry.path())
                .flat_map(|path| {
                    let segment: Result<Segment, SegmentError> = path.as_path().try_into();
                    segment.into_iter()
                }).sorted_by_key(|s| s.header.segment_id)
                .for_each(|s| segments.push(s));
            let current_segment = Segment::new(segments.len() as u64, path).expect("Can't create new segment");
            segments.push(current_segment);
        }
        let len = segments.len();
        info!("Segments loading is finished");
        Ok(Storage { segments, current_index: len - 1, config })
    }

    pub fn insert(&mut self, key: &ByteStr, value: &ByteStr) -> Result<(), SegmentError> {
        let current_segment = &mut self.segments[self.current_index];
        debug!("Inserting key: {:?} in segment: {}", key, self.current_index);
        let position = current_segment.insert(key, value)?;
        if position > FILE_SIZE_LIMIT {
            self.compact()?;
        }
        Ok(())
    }

    pub fn get(&self, key: &ByteStr) -> io::Result<Option<ByteString>> {
        for seg_idx in (0..=self.current_index).rev() {
            debug!("Looking for key: {:?} in segment: {}", key, seg_idx);
            let result = self.segments[seg_idx].get(key)?;
            if result.is_some() {
                debug!("Key is found in segment: {}", seg_idx);
                return Ok(result);
            }
        }
        Ok(None)
    }

    #[inline]
    pub fn update(&mut self, key: &ByteStr, value: &ByteStr) -> Result<(), SegmentError> {
        self.insert(key, value)
    }
    #[inline]
    pub fn delete(&mut self, key: &ByteStr) -> Result<(), SegmentError> {
        self.insert(key, &[0])
    }


    fn tmp_dir_path(base_path: &Path) -> PathBuf {
        let mut path_buf = base_path.to_path_buf();
        path_buf.push("tmp");
        path_buf
    }
    fn compact(&mut self) -> Result<(), SegmentError> {
        info!("Compaction started");
        let path = PathBuf::from(&self.config.base_path);
        let prev_segment_path = Segment::data_path(self.segments[self.current_index].header.segment_id, false, &path);
        &self.segments[self.current_index].compact(&Storage::tmp_dir_path(&path))?;
        self.current_index += 1;

        let current_segment = Segment::new(self.current_index as u64, &path)?;
        self.segments.push(current_segment);

        let tmp_dir = Storage::tmp_dir_path(&path);
        fs::read_dir(&tmp_dir)?
            .for_each(|file| {
                file.map(|f| {
                    let file_name = f.file_name();
                    let mut new_path = PathBuf::from(&self.config.base_path);
                    new_path.push(file_name);
                    (f.path(), new_path)
                }).and_then(|(old_path, new_path)| {
                    debug!("Moving {:?} from tmp. new path {:?}", old_path, new_path);
                    fs::rename(&old_path, &new_path)
                })
                    .expect("Can't move files from temporary folder");
            });
        fs::remove_file(&prev_segment_path)?;
        info!("Compaction finished");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::env::temp_dir;
    use std::fs;
    use std::fs::File;
    use std::fs::OpenOptions;

    use crate::config::Config;
    use crate::segment::Segment;
    use crate::Storage;

    #[test]
    fn storage_compact() {
        let mut dir = temp_dir();


        dir.push("storage_test");
        if dir.exists() {
            fs::remove_dir_all(&dir).unwrap();
            fs::create_dir(&dir).unwrap();
        } else {
            fs::create_dir(&dir).unwrap();
        }

        let mut config = Config {
            base_path: dir.to_string_lossy().to_string()
        };
        let mut storage = Storage::open(config).unwrap();

        storage.insert(&String::from("key").into_bytes(), &String::from("value").into_bytes());
        storage.insert(&String::from("key").into_bytes(), &String::from("value").into_bytes());
        storage.insert(&String::from("key").into_bytes(), &String::from("value").into_bytes());
        storage.insert(&String::from("key").into_bytes(), &String::from("value").into_bytes());

        storage.compact();
        storage.insert(&String::from("key2").into_bytes(), &String::from("value2").into_bytes());
        let result = storage.get(&String::from("key").into_bytes()).unwrap();
        assert!(result.is_some());
        assert_eq!(String::from("value").into_bytes(), result.unwrap());
    }
}

