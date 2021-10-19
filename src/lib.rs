extern crate byteorder;
extern crate config as config_lib;
extern crate crc;
extern crate serde_derive;

use std::cmp::Ordering;
use std::fs;
use std::fs::{DirEntry, OpenOptions};
use std::io;
use std::io::Error;
use std::path::PathBuf;

use itertools::Itertools;
use log::{debug, error, info, warn};

use crate::config::Config;
use crate::segment::ByteStr;
use crate::segment::ByteString;
use crate::segment::Segment;
use crate::segment::SegmentError;

pub mod segment;
pub mod command;
pub mod config;

const FILE_SIZE_LIMIT: u64 = 2 * 1024 * 1024;

#[derive(Eq, Ord)]
enum FileType {
    Compacted(PathBuf),
    Index(PathBuf),
    Full(PathBuf)
}

impl PartialEq<Self> for FileType {
    fn eq(&self, _: &Self) -> bool {
        false
    }
}

impl PartialOrd for FileType {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self {
            FileType::Full(_) => Some(Ordering::Less),
            FileType::Compacted(_) => {
                match other {
                    FileType::Index(_) => Some(Ordering::Less),
                    FileType::Full(_) => Some(Ordering::Greater),
                    _ => None
                }
            },
            FileType::Index(_) => {
                match other {
                    FileType::Compacted(_) => Some(Ordering::Greater),
                    FileType::Full(_) => Some(Ordering::Greater),
                    _ => None
                }
            }
        }
    }
}


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
            let mut path_buf = path.to_path_buf();
            path_buf.push("seg.0");
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .append(true)
                .open(path_buf)?;
            segments.push(Segment::from_file(f));
        } else {
            let grouped = files.into_iter()
                .map(|dir_entry| dir_entry.path())
                .map(|path| {
                    let file_name = path.file_name().unwrap().to_str().unwrap();
                    let parts: Vec<&str> = file_name.split('.').collect();
                    (parts[1].to_string(), path)
                })
                .sorted_by_key(|p| p.0.parse::<u32>().unwrap())
                .group_by(|(_, dir_entry)| {
                    let file_name = dir_entry.file_name().unwrap().to_str().unwrap();
                    let parts: Vec<&str> = file_name.split('.').collect();
                    parts[1].to_string()
                });

            for (_, group) in &grouped {
                let files_with_types: Vec<FileType> = group.map(|(_, pp)| {
                    debug!("Found segment file: {:?}", pp);
                    let ext = pp.extension().and_then(|s| s.to_str());
                    match ext {
                        None => panic!("Wrong file name"),
                        Some("index") => FileType::Index(pp),
                        Some("compacted") => {
                            FileType::Compacted(pp)
                        },
                        Some(_) => {
                            FileType::Full(pp)
                        }
                    }
                }).sorted().collect();
                segments.push(Storage::create_segments(&files_with_types).unwrap());
                debug!("group finished");
            }

        }
        let len = segments.len();
        info!("Segments loading is finished");
        Ok(Storage { segments, current_index: len - 1, config })

    }

    pub fn insert(&mut self, key: &ByteStr, value: &ByteStr) -> Result<(), SegmentError> {
        let current_segment = &mut self.segments[self.current_index];
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

    fn construct_path(base_path: &str, extension: &str, index: &usize) -> PathBuf {
        let mut path_buf = Storage::tmp_dir_path(base_path);
        path_buf.push(String::from("seg.") + &index.to_string() +".");
        path_buf.set_extension(extension);
        path_buf
    }

    fn current_segment_path(&self) -> PathBuf {
        let mut path_buf = PathBuf::from(&self.config.base_path);
        path_buf.push(String::from("seg"));
        path_buf.set_extension(self.current_index.to_string());
        path_buf
    }
    fn tmp_dir_path(base_path: &str) -> PathBuf {
        let mut path_buf = PathBuf::from(base_path);
        path_buf.push("tmp");
        path_buf
    }
    fn compact(&mut self) -> Result<(), SegmentError> {
        info!("Compaction started");
        let compacted_path = Storage::construct_path(&self.config.base_path, "compacted", &self.current_index);
        let index_path = Storage::construct_path(&self.config.base_path, "index", &self.current_index);
        let prev_segment_path = self.current_segment_path();
        self.segments[self.current_index].compact(&compacted_path, &index_path)?;
        self.current_index += 1;

        let current_segment_path = self.current_segment_path();
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(current_segment_path)?;

        self.segments.push(Segment::from_file(f));

        let tmp_dir = Storage::tmp_dir_path(&self.config.base_path);
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

    fn create_segments(files_with_types: &Vec<FileType>) -> Result<Segment, ()> {
        if files_with_types.len() == 1 {
            let seg = match files_with_types.get(0)  {
                Some(FileType::Full(path_buf)) => {
                    let f = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .append(true)
                        .open(path_buf).unwrap();
                    let mut segment = Segment::from_file(f);
                    segment.load().unwrap();
                    segment
                },
                _ => panic!("something went wrong")
            };
            Ok(seg)
        } else {
            let seg = match files_with_types.get(0)  {
                Some(FileType::Compacted(buf)) => {
                    match files_with_types.get(1)  {
                        Some(FileType::Index(path)) => {
                            let f = OpenOptions::new()
                                .read(true)
                                .open(buf).unwrap();
                            Segment::from_file_with_index(f, path)
                        },
                        _ => panic!(""),
                    }
                },
                _ => panic!(""),
            };
            Ok(seg)
        }
    }

}
#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::env::temp_dir;
    use std::fs::File;
    use std::fs::OpenOptions;
    use std::fs;
    use crate::segment::Segment;
    use crate::Storage;
    use crate::config::Config;

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

        storage.insert( &String::from("key").into_bytes(), &String::from("value").into_bytes());
        storage.insert( &String::from("key").into_bytes(), &String::from("value").into_bytes());
        storage.insert( &String::from("key").into_bytes(), &String::from("value").into_bytes());
        storage.insert( &String::from("key").into_bytes(), &String::from("value").into_bytes());

        storage.compact();
        storage.insert( &String::from("key2").into_bytes(), &String::from("value2").into_bytes());
        let result = storage.get(&String::from("key").into_bytes()).unwrap();
        assert!(result.is_some());
        assert_eq!(String::from("value").into_bytes(), result.unwrap());
    }
}

