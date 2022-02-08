use std::convert::TryFrom;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Cursor, ErrorKind, Read, Seek, SeekFrom, Write};
use std::{fs, io};
use std::path::PathBuf;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::crc32;

use crate::memtable::ByteString;

//TODO Error handling
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
enum CommandType {
    Insert = 1,
    Remove = 2,
}

impl TryFrom<u8> for CommandType {
    type Error = io::Error;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(CommandType::Insert),
            2 => Ok(CommandType::Remove),
            //FIXME:
            _ => Err(io::Error::from_raw_os_error(1))
        }
    }
}

impl Into<u8> for CommandType {
    fn into(self) -> u8 {
        match self {
            CommandType::Insert => 1,
            CommandType::Remove => 2,
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
pub enum LogRecord {
    Remove(ByteString),
    Insert(ByteString, ByteString),
}


pub struct CommandLog<T: Read + Write> {
    file: T,
    path: Option<PathBuf>
}

impl<T: Read + Write> Write for CommandLog<T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl<T: Read + Write> Read for CommandLog<T> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.file.read(buf)
    }
}

impl<T: Read + Write> Iterator for &mut CommandLog<T> {
    type Item = Result<LogRecord, io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.next_record();
        match result {
            Ok(record) => Some(Ok(record)),
            Err(err) => match err.kind() {
                io::ErrorKind::UnexpectedEof => {
                    None
                }
                _ => Some(Err(err)),
            }
        }
    }

}
impl CommandLog<Cursor<Vec<u8>>> {
    pub fn new_in_memory(vec: Vec<u8>) -> CommandLog<Cursor<Vec<u8>>> {
        let mut cursor = Cursor::new(vec);
        cursor.seek(SeekFrom::Start(0)).unwrap();
        CommandLog{
            file: cursor,
            path: None,
        }
    }
    pub fn inner(self) -> Vec<u8>{
        self.file.into_inner()
    }
}
impl CommandLog<File> {
    pub fn new(path: PathBuf) -> io::Result<CommandLog<File>> {
        // let path = PathBuf::from("./wal.log");
        let mut new_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&path)?;
        new_file.seek(SeekFrom::Start(0))?;
        Ok(CommandLog {
            file: new_file,
            path: Some(path)
        })
    }

    pub fn close(&self) -> io::Result<()> {
        match &self.path {
            Some(val) => fs::remove_file(val),
            None => Ok(())
        }
    }

}
impl<T: Read + Write> CommandLog<T> {


    pub fn insert(&mut self, key: &ByteString, val: &ByteString) -> io::Result<usize> {
        let record = LogRecord::Insert(key.to_vec(), val.to_vec());
        self.log(&record)
    }

    pub fn remove(&mut self, key: &ByteString) -> io::Result<usize> {
        let record = LogRecord::Remove(key.to_vec());
        self.log(&record)
    }

    fn next_record(&mut self) -> io::Result<LogRecord> {
        // let mut f = BufReader::new(self);
        let command: CommandType = CommandType::try_from(self.read_u8()?)?;
        let saved_checksum = self.read_u32::<LittleEndian>()?;
        match command {
            CommandType::Insert => {
                let key_len = self.read_u32::<LittleEndian>()?;
                let val_len = self.read_u32::<LittleEndian>()?;
                let data_len = key_len + val_len;
                let mut data = ByteString::with_capacity(data_len as usize);
                {
                    Read::take(self,data_len as u64)
                        .read_to_end(&mut data)?;
                }
                debug_assert_eq!(data.len(), data_len as usize);
                let checksum = crc::crc32::checksum_ieee(&data);
                if checksum != saved_checksum {
                    return Err(io::Error::new(ErrorKind::InvalidData, format!("data corruption encountered ({:08x}) != {:08x}", checksum, saved_checksum)));
                }
                let val = data.split_off(key_len as usize);
                let key = data;
                Ok(LogRecord::Insert(key, val))
            }
            CommandType::Remove => {
                let key_len = self.read_u32::<LittleEndian>()?;
                let mut data = ByteString::with_capacity(key_len as usize);
                {
                    Read::take(self, key_len as u64)
                        .read_to_end(&mut data)?;
                }
                debug_assert_eq!(data.len(), key_len as usize);
                let checksum = crc::crc32::checksum_ieee(&data);
                if checksum != saved_checksum {
                    panic!("data corruption encountered ({:08x}) != {:08x}", checksum, saved_checksum);
                }
                Ok(LogRecord::Remove(data))
            }
        }
    }

    pub fn log(&mut self, record: &LogRecord) -> io::Result<usize> {
        let mut f = BufWriter::new(self);
        match record {
            LogRecord::Insert(key, val) => {
                let data_len = key.len() + val.len();
                let mut tmp = ByteString::with_capacity(data_len);
                for byte in key {
                    tmp.push(*byte);
                }
                for byte in val {
                    tmp.push(*byte);
                }
                let checksum = crc32::checksum_ieee(&tmp);
                f.write_u8(CommandType::Insert as u8)?;
                f.write_u32::<LittleEndian>(checksum)?;
                f.write_u32::<LittleEndian>(key.len() as u32)?;
                f.write_u32::<LittleEndian>(val.len() as u32)?;
                f.write_all(&tmp)?;
                f.flush()?;
                Ok(data_len + 5)
            }
            LogRecord::Remove(key) => {
                let checksum = crc32::checksum_ieee(key);
                f.write_u8(CommandType::Remove as u8)?;
                f.write_u32::<LittleEndian>(checksum)?;
                f.write_u32::<LittleEndian>(key.len() as u32)?;
                f.write_all(key)?;
                f.flush()?;
                Ok(key.len() + 5)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::read;
    use crate::wal::{CommandLog, LogRecord};

    #[test]
    fn write_insert_log_record() {
        let mut log = CommandLog::new_in_memory(Vec::new());
        log.insert("key".as_bytes().to_vec().as_ref(), "value".as_bytes().to_vec().as_ref()).unwrap();
        let expected_record = LogRecord::Insert("key".as_bytes().to_vec(), "value".as_bytes().to_vec());
        let mut read_log = CommandLog::new_in_memory(log.inner());
        let actual_record = read_log.next_record().unwrap();
        assert_eq!(expected_record, actual_record);
    }


    #[test]
    fn write_remove_log_record() {
        let mut log = CommandLog::new_in_memory(Vec::new());
        log.remove("key".as_bytes().to_vec().as_ref()).unwrap();
        let expected_record = LogRecord::Remove("key".as_bytes().to_vec());
        let mut read_log = CommandLog::new_in_memory(log.inner());
        let actual_record = read_log.next_record().unwrap();
        assert_eq!(expected_record, actual_record);
    }
}