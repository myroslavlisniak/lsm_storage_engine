use std::convert::TryFrom;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::io;
use std::path::PathBuf;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::crc32;

use crate::memtable::ByteString;
//TODO Error handling
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
enum Command {
    Insert = 1,
    Update = 2,
    Remove = 3,
    Get = 4,
}

impl TryFrom<u8> for Command {
    type Error = io::Error;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Command::Insert),
            2 => Ok(Command::Update),
            3 => Ok(Command::Remove),
            4 => Ok(Command::Get),
            //FIXME:
            _ => Err(io::Error::from_raw_os_error(1))
        }
    }
}

impl Into<u8> for Command {
    fn into(self) -> u8 {
        match self {
            Command::Insert => 1,
            Command::Update => 2,
            Command::Remove => 3,
            Command::Get => 4
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
enum Data {
    KeyOnly(ByteString),
    KeyVal(ByteString, ByteString),
}

#[derive(PartialEq, Eq, Debug)]
struct LogRecord {
    command: Command,
    data: Data,
}

pub struct CommandLog {
    file: File,
}

impl LogRecord {
    pub fn write<W: Write>(&self, writer: &mut W) -> io::Result<usize> {
        let mut f = BufWriter::new(writer);
        match self.command {
            Command::Insert | Command::Update => {
                match &self.data {
                    Data::KeyVal(key, val) => {
                        let data_len = key.len() + val.len();
                        let mut tmp = ByteString::with_capacity(data_len);
                        for byte in key {
                            tmp.push(*byte);
                        }
                        for byte in val {
                            tmp.push(*byte);
                        }
                        let checksum = crc32::checksum_ieee(&tmp);
                        f.write_u8(self.command.into());
                        f.write_u32::<LittleEndian>(checksum)?;
                        f.write_u32::<LittleEndian>(key.len() as u32)?;
                        f.write_u32::<LittleEndian>(val.len() as u32)?;
                        f.write_all(&tmp)?;
                        Ok(data_len + 5)
                    }
                    Data::KeyOnly(_) => panic!("Command type does not match with data")
                }
            }
            Command::Get | Command::Remove => {
                match &self.data {
                    Data::KeyVal(_, _) => panic!("Command type does not match with data"),
                    Data::KeyOnly(key) => {
                        let checksum = crc32::checksum_ieee(key);
                        f.write_u8(self.command.into());
                        f.write_u32::<LittleEndian>(checksum)?;
                        f.write_u32::<LittleEndian>(key.len() as u32)?;
                        f.write_all(key)?;
                        Ok(key.len() + 5)
                    }
                }
            }
        }
    }

    pub fn read_from<R: Read>(r: &mut R) -> io::Result<LogRecord> {
        let mut f = BufReader::new(r);
        let command: Command = Command::try_from(f.read_u8()?)?;
        let saved_checksum = f.read_u32::<LittleEndian>()?;
        match command {
            Command::Insert | Command::Update => {
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
                Ok(LogRecord {
                    command,
                    data: Data::KeyVal(key, val),
                })
            }
            Command::Get | Command::Remove => {
                let key_len = f.read_u32::<LittleEndian>()?;
                let mut data = ByteString::with_capacity(key_len as usize);
                {
                    f.by_ref()
                        .take(key_len as u64)
                        .read_to_end(&mut data)?;
                }
                debug_assert_eq!(data.len(), key_len as usize);
                let checksum = crc::crc32::checksum_ieee(&data);
                if checksum != saved_checksum {
                    panic!("data corruption encountered ({:08x}) != {:08x}", checksum, saved_checksum);
                }
                Ok(LogRecord {
                    command,
                    data: Data::KeyOnly(data),
                })
            }
        }
    }
}

impl Write for CommandLog {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl Read for CommandLog {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.file.read(buf)
    }
}

impl CommandLog {
    pub fn new () -> CommandLog {
        let path = PathBuf::from("./wal.log");
        let mut new_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path).expect("Can't create/open wal.log file");
        CommandLog {
            file: new_file
        }
    }
    pub fn insert(&mut self, key: ByteString, val: ByteString) -> io::Result<usize> {
        let record = LogRecord {
            command: Command::Insert,
            data: Data::KeyVal(key, val),
        };
        record.write(self)
    }
    pub fn update(&mut self, key: ByteString, val: ByteString) -> io::Result<usize> {
        let record = LogRecord {
            command: Command::Update,
            data: Data::KeyVal(key, val),
        };
        record.write( self)
    }
    pub fn remove(&mut self, key: ByteString) -> io::Result<usize> {
        let record = LogRecord {
            command: Command::Remove,
            data: Data::KeyOnly(key),
        };
        record.write( self)
    }
}

#[cfg(test)]
mod tests {
    use crate::ByteString;
    use crate::wal::{Command, Data, LogRecord};

    #[test]
    fn write_insert_log_record() {
        let expected_record = LogRecord {
            command: Command::Insert,
            data: Data::KeyVal("key".as_bytes().to_vec(), "value".as_bytes().to_vec()),
        };
        let mut buff: ByteString = Vec::new();
        expected_record.write(&mut buff);
        let actual_record = LogRecord::read_from(&mut buff.as_slice()).unwrap();
        assert_eq!(expected_record, actual_record);
    }

    #[test]
    fn write_update_log_record() {
        let expected_record = LogRecord {
            command: Command::Update,
            data: Data::KeyVal("key".as_bytes().to_vec(), "value".as_bytes().to_vec()),
        };
        let mut buff: ByteString = Vec::new();
        expected_record.write(&mut buff);
        let actual_record = LogRecord::read_from(&mut buff.as_slice()).unwrap();
        assert_eq!(expected_record, actual_record);
    }

    #[test]
    fn write_get_log_record() {
        let expected_record = LogRecord {
            command: Command::Get,
            data: Data::KeyOnly("key".as_bytes().to_vec()),
        };
        let mut buff: ByteString = Vec::new();
        expected_record.write(&mut buff);
        let actual_record = LogRecord::read_from(&mut buff.as_slice()).unwrap();
        assert_eq!(expected_record, actual_record);
    }

    #[test]
    fn write_remove_log_record() {
        let expected_record = LogRecord {
            command: Command::Remove,
            data: Data::KeyOnly("key".as_bytes().to_vec()),
        };
        let mut buff: ByteString = Vec::new();
        expected_record.write(&mut buff);
        let actual_record = LogRecord::read_from(&mut buff.as_slice()).unwrap();
        assert_eq!(expected_record, actual_record);
    }
}