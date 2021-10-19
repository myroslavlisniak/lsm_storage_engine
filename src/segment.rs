use std::fs::{File, OpenOptions};
use std::collections::HashMap;
use std::io::{BufReader, BufWriter, Error, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::io;
use std::fmt;
use std::fmt::Formatter;
use bincode::ErrorKind;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::crc32;
use serde_derive::{Deserialize, Serialize};
use log::{debug, error, info, warn};

pub type ByteStr = [u8];
pub type ByteString = Vec<u8>;

#[derive(Debug, Serialize, Deserialize)]
pub struct KeyValuePair {
    pub key: ByteString,
    pub value: ByteString,
}
#[derive(Debug)]
pub struct SegmentError {
    kind: String,
    message: String
}

#[derive(Debug)]
pub struct Segment {
    file: File,
    index: HashMap<ByteString, u64>,
}

impl fmt::Display for SegmentError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "An Error Occurred, Please Try Again!")
    }
}
impl From<io::Error> for SegmentError {
    fn from(e: Error) -> Self {
        SegmentError{
            kind: String::from("io"),
            message: e.to_string()
        }
    }
}

impl From<Box<bincode::ErrorKind>> for SegmentError {
    fn from(e: Box<ErrorKind>) -> Self {
        SegmentError{
            kind: String::from("index"),
            message: e.to_string()
        }
    }
}

impl Segment {
    pub fn from_file(file: File) -> Segment {
        Segment{
            file,
            index: HashMap::new()
        }
    }

    pub fn from_file_with_index(file: File, index_path: &Path) -> Segment {
        let mut segment = Segment {
            file,
            index: Segment::read_index(index_path).unwrap_or_else(|_| HashMap::new()),
        };
        if segment.index.is_empty() {
            segment.load().expect("Can't process data file. data file is corrupted");
        }
        segment

    }

    pub fn load(&mut self) -> io::Result<()> {
        let mut f = BufReader::new(&mut self.file);
        f.seek(SeekFrom::Start(0));
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
            Some(position) =>  {
                let kv = self.get_at(*position)?;
                if kv.value == [0] {
                    Ok(None)
                } else {
                    Ok(Some(ByteString::from(kv.value)))
                }
            }
        }
    }



    pub fn compact(&mut self, path: &Path, index_path: &Path) -> Result<(), SegmentError> {
        let mut f = BufReader::new(&mut self.file);
        let mut new_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path)?;
        f.seek(SeekFrom::Start(0))?;

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
        self.change_segment_file(new_file);
        self.index.clear();
        self.load()?;
        self.save_index(index_path)?;
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

    fn change_segment_file(&mut self, new_file: File) {
        self.file = new_file;
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
        debug!("read data");
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
    use std::fs::File;
    use std::fs::OpenOptions;
    use std::fs;
    use crate::segment::Segment;

    #[test]
    fn segment_compact() {
        let mut dir = temp_dir();

        let name = "seg.0";
        dir.push(name);
        fs::remove_file(&dir);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&dir).unwrap();
        let mut segment = Segment{
            file,
            index: HashMap::new()
        };
        segment.insert( &String::from("key").into_bytes(), &String::from("value").into_bytes());
        segment.insert( &String::from("key").into_bytes(), &String::from("value").into_bytes());
        segment.insert( &String::from("key").into_bytes(), &String::from("value").into_bytes());
        segment.insert( &String::from("key").into_bytes(), &String::from("value").into_bytes());
        let mut dir = temp_dir();
        let mut compacted_path = dir.clone();
        compacted_path.push("seg.0.compacted");
        let mut index_path = dir.clone();
        index_path.push("seg.0.index");
        fs::remove_file(&compacted_path);
        fs::remove_file(&index_path);
        let cc = segment.compact(&compacted_path, &index_path);

        // let compacted_file = File::open(compacted_path).unwrap();
        // let mut compacted_segment = Segment::from_file_with_index(compacted_file, &index_path);
        let result = segment.get(&String::from("key").into_bytes()).unwrap();
        assert!(result.is_some());
        assert_eq!(String::from("value").into_bytes(), result.unwrap());
    }
}
