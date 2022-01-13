use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt;
use std::fmt::Formatter;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Error, Read, Seek, SeekFrom, Write};
use std::io;
use std::path::{Path, PathBuf};
use std::str;
use std::time::{SystemTime, UNIX_EPOCH};

use bincode::ErrorKind;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::crc32;
use log::{debug, error, info, warn};
use serde_derive::{Deserialize, Serialize};

pub type ByteStr = [u8];
pub type ByteString = Vec<u8>;

#[derive(Debug, Serialize, Deserialize)]
pub struct KeyValuePair {
    pub key: ByteString,
    pub value: ByteString,
}


#[derive(Debug, Serialize, Deserialize)]
pub struct SegmentHeader {
    pub segment_id: u64,
    creation_date: u128,
    compacted: bool,
}

#[derive(Debug)]
pub struct Segment {
    pub header: SegmentHeader,
    file: File,
    index: HashMap<ByteString, u64>,
}

impl fmt::Display for SegmentError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message())
    }
}

impl fmt::Debug for SegmentError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message())
    }
}

impl From<io::Error> for SegmentError {
    fn from(e: Error) -> Self {
        Self::IoError(e.to_string())
    }
}


impl From<Box<bincode::ErrorKind>> for SegmentError {
    fn from(_: Box<ErrorKind>) -> Self {
        SegmentError::CorruptedIndex
    }
}

pub enum SegmentError {
    IoError(String),
    MissingHeader,
    CorruptedIndex,
}


impl SegmentError {
    fn message(&self) -> String {
        match self {
            Self::IoError(message) => format!("Io error occured. {} ", message),
            Self::MissingHeader => format!("Header is missing"),
            Self::CorruptedIndex => format!("Corrupted index found, skipping")
        }
    }
}

impl std::error::Error for SegmentError {}

impl TryFrom<&Path> for Segment {
    type Error = SegmentError;

    fn try_from(path: &Path) -> Result<Self, Self::Error> {
        let file = OpenOptions::new()
            .read(true)
            .open(path)?;
        let mut reader = BufReader::new(&file);
        let header = SegmentHeader::from_reader(&mut reader)?;
        let parent_path = path.parent().unwrap();
        let index_path = Segment::index_path(header.segment_id, parent_path);
        let index = Segment::read_index(&index_path).unwrap_or(HashMap::new());

        let mut segment = Segment {
            header,
            file,
            index,
        };
        if segment.index.is_empty() {
            segment.load()?;
        }
        Ok(segment)
    }
}

impl SegmentHeader {
    fn from_reader<R: BufRead>(reader: &mut R) -> Result<SegmentHeader, SegmentError> {
        let mut input = String::new();
        reader.read_line(&mut input)?;
        let result: SegmentHeader = serde_json::from_str(&input)
            .or(Err(SegmentError::MissingHeader))?;
        Ok(result)
    }

    fn to_writer<W: Write>(&self, writer: &mut W) -> Result<(), SegmentError> {
        let mut string = serde_json::to_string(self).expect("Can't serialize header");
        string.push_str("\n");
        writer.write_all(string.as_bytes())?;
        // writer.write_all("\n".as_bytes())?;
        Ok(())
    }
}

impl Segment {
    pub fn new(id: u64, path: &Path) -> Result<Segment, SegmentError> {
        let path_buf = Segment::data_path(id, false, path);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&path_buf)?;

        let header = SegmentHeader {
            segment_id: id,
            compacted: false,
            creation_date: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_millis(),
        };
        header.to_writer(&mut file)?;
        Ok(Segment {
            header,
            file,
            index: HashMap::new(),
        })
    }
    pub fn index_path(id: u64, base_path: &Path) -> PathBuf {
        let mut path = base_path.to_path_buf();
        path.push(format!("segment.{}.index", id));
        path
    }

    pub fn data_path(id: u64, compacted: bool, base_path: &Path) -> PathBuf {
        let mut path = base_path.to_path_buf();
        let mut string = format!("segment.{}", id);
        if compacted {
            string.push_str(".compacted");
        }
        path.push(string);
        path
    }


    pub fn load(&mut self) -> io::Result<()> {
        let mut f = BufReader::new(&mut self.file);
        f.seek(SeekFrom::Start(0))?;
        f.read_line(&mut String::new())?;
        loop {
            let current_position = f.seek(SeekFrom::Current(0))?;
            let maybe_kv = Segment::process_record(&mut f);
            let kv = match maybe_kv {
                Ok(kv) => kv,
                Err(err) =>
                    match err.kind() {
                        io::ErrorKind::UnexpectedEof => {
                            break;
                        }
                        _ => return Err(err)
                    }
            };
            self.index.insert(kv.key, current_position);
        }
        Ok(())
    }

    pub fn insert(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<u64> {
        let position = Segment::insert_but_ignore_index(key, value, &mut self.file)?;
        self.index.insert(key.to_vec(), position);
        Ok(position)
    }

    pub fn get(&self, key: &ByteStr) -> io::Result<Option<ByteString>> {
        match self.index.get(key) {
            None => Ok(None),
            Some(position) => {
                let kv = self.get_at(*position)?;
                if kv.value == [0] {
                    Ok(None)
                } else {
                    Ok(Some(ByteString::from(kv.value)))
                }
            }
        }
    }


    pub fn compact(&mut self, base_path: &Path) -> Result<(), SegmentError> {
        let mut f = BufReader::new(&mut self.file);
        let data_path = Segment::data_path(self.header.segment_id, true, base_path);
        let index_path = Segment::index_path(self.header.segment_id, base_path);
        let mut new_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(data_path)?;
        let header = SegmentHeader {
            segment_id: self.header.segment_id,
            creation_date: self.header.creation_date,
            compacted: true,
        };
        header.to_writer(&mut new_file)?;
        f.seek(SeekFrom::Start(0))?;
        f.read_line(&mut String::new())?;
        loop {
            let current_position = f.seek(SeekFrom::Current(0))?;
            let maybe_kv = Segment::process_record(&mut f);
            let kv = match maybe_kv {
                Ok(kv) => kv,
                Err(err) =>
                    match err.kind() {
                        io::ErrorKind::UnexpectedEof => {
                            break;
                        }
                        _ => return Err(SegmentError::from(err)),
                    }
            };
            match self.index.get(&kv.key) {
                None => panic!("Corrupted files detected"),
                Some(offset) => {
                    if *offset == current_position {
                        if kv.value != [0] {
                            Segment::insert_but_ignore_index(&kv.key, &kv.value, &mut new_file)?;
                        } else {
                            self.index.remove(&kv.key);
                        }
                    }
                }
            }
        }
        self.file = new_file;
        self.index.clear();
        self.load()?;
        self.save_index(&index_path)?;
        Ok(())
    }

    fn read_index(path: &Path) -> Result<HashMap<ByteString, u64>, SegmentError> {
        let mut f = OpenOptions::new()
            .read(true)
            .open(path)?;
        let size = f.metadata()?.len();
        let mut content: ByteString = ByteString::with_capacity(size as usize);
        let mut f = BufReader::new(&mut f);
        f.read_to_end(&mut content)?;
        let index: HashMap<ByteString, u64> = bincode::deserialize(&content)?;
        Ok(index)
    }

    fn save_index(&mut self, path: &Path) -> Result<(), SegmentError> {
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .open(path)?;
        let bytes = bincode::serialize(&self.index).unwrap();
        let mut f = BufWriter::new(&mut f);
        f.write_all(&bytes).map_err(&SegmentError::from)
    }


    fn insert_but_ignore_index(key: &ByteStr, value: &ByteStr, file: &mut File) -> io::Result<u64> {
        let mut f = BufWriter::new(file);
        let key_len = key.len();
        let val_len = value.len();
        let mut tmp = ByteString::with_capacity(key_len + val_len);
        for byte in key {
            tmp.push(*byte);
        }
        for byte in value {
            tmp.push(*byte);
        }
        let checksum = crc32::checksum_ieee(&tmp);
        let next_byte = SeekFrom::End(0);
        let current_position = f.seek(SeekFrom::Current(0))?;
        f.seek(next_byte)?;
        f.write_u32::<LittleEndian>(checksum)?;
        f.write_u32::<LittleEndian>(key_len as u32)?;
        f.write_u32::<LittleEndian>(val_len as u32)?;
        f.write_all(&tmp)?;
        Ok(current_position)
    }

    fn process_record<R: Read>(f: &mut R) -> io::Result<KeyValuePair> {
        let saved_checksum = f.read_u32::<LittleEndian>()?;
        let key_len = f.read_u32::<LittleEndian>()?;
        let val_len = f.read_u32::<LittleEndian>()?;
        let data_len = key_len + val_len;
        let mut data = ByteString::with_capacity(data_len as usize);
        {
            f.by_ref()
                .take(data_len as u64)
                .read_to_end(&mut data)?;
        }
        debug_assert_eq!(data.len(), data_len as usize);
        let checksum = crc::crc32::checksum_ieee(&data);
        if checksum != saved_checksum {
            panic!("data corruption encountered ({:08x}) != {:08x}", checksum, saved_checksum);
        }
        let val = data.split_off(key_len as usize);
        let key = data;
        Ok(KeyValuePair {
            key,
            value: val,
        })
    }
    fn get_at(&self, position: u64) -> io::Result<KeyValuePair> {
        let mut f = BufReader::new(&self.file);
        debug!("seeking into segment file, position {}", position);
        f.seek(SeekFrom::Start(position)).unwrap();
        Segment::process_record(&mut f)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::env::temp_dir;
    use std::fs;
    use std::fs::File;
    use std::fs::OpenOptions;

    use crate::segment::Segment;
    use crate::segment::SegmentHeader;

    #[test]
    fn segment_compact() {
        let mut dir = temp_dir();

        let file = Segment::data_path(1, false, &dir);
        fs::remove_file(&file);
        let header = SegmentHeader {
            segment_id: 1,
            creation_date: 10000000,
            compacted: false,
        };
        let mut segment = Segment::new(1, &dir).unwrap();
        segment.insert(&String::from("key").into_bytes(), &String::from("value").into_bytes());
        segment.insert(&String::from("key").into_bytes(), &String::from("value").into_bytes());
        segment.insert(&String::from("key").into_bytes(), &String::from("value").into_bytes());
        segment.insert(&String::from("key").into_bytes(), &String::from("value").into_bytes());
        let compacted_path = Segment::data_path(1, true, &dir);
        let index_path = Segment::index_path(1, &dir);
        fs::remove_file(&compacted_path);
        fs::remove_file(&index_path);
        let cc = segment.compact(&dir);

        // let compacted_file = File::open(compacted_path).unwrap();
        // let mut compacted_segment = Segment::from_file_with_index(compacted_file, &index_path);
        let result = segment.get(&String::from("key").into_bytes()).unwrap();
        assert!(result.is_some());
        assert_eq!(String::from("value").into_bytes(), result.unwrap());
    }
}
