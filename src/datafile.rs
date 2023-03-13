use std::fs::{File, OpenOptions};
use std::io;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::Path;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crate::{ByteStr, KeyValuePair};

pub(crate) struct WriteableDataFile {
    data: File
}

pub(crate) struct ReadOnlyDataFile {
    data: File,
}

impl WriteableDataFile {
    pub(crate) fn open(path: &Path) -> io::Result<WriteableDataFile> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&path)?;
        Ok(WriteableDataFile {
            data: file
        })
    }

    pub(crate) fn write_key_value(&mut self, key: &ByteStr, val: &ByteStr) -> io::Result<u64> {
        let key_len = key.len() as u32;
        let val_len = val.len() as u32;
        self.write_u32::<LittleEndian>(key_len)?;
        self.write_u32::<LittleEndian>(val_len)?;
        self.write_all(key)?;
        self.write_all(val)?;
        Ok(u64::from(8 + key_len + val_len))
    }

}

impl Write for WriteableDataFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.data.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.data.flush()
    }
}

impl Seek for WriteableDataFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.data.seek(pos)
    }
}

impl ReadOnlyDataFile {
    pub(crate) fn open(path: &Path) -> io::Result<ReadOnlyDataFile> {
        let file = OpenOptions::new()
            .read(true)
            .open(path)?;
        Ok(ReadOnlyDataFile {
            data: file,
        })
    }

    pub(crate) fn read_record(&mut self, pos: u64) -> io::Result<Option<(KeyValuePair, u64)>> {
        match self.read_record_unsafe(pos) {
            Ok(res) => Ok(Some(res)),
            Err(err) => match err.kind() {
                ErrorKind::UnexpectedEof => Ok(None),
                _ => Err(err)
            }
        }
    }

    fn read_record_unsafe(&mut self, pos: u64) -> io::Result<(KeyValuePair, u64)> {
        let seek_from = SeekFrom::Start(pos);
        self.data.seek(seek_from)?;
        let key_len = self.data.read_u32::<LittleEndian>()?;
        let val_len = self.data.read_u32::<LittleEndian>()?;
        let mut key: Vec<u8> = vec![0u8; key_len as usize];
        let mut val: Vec<u8> = vec![0u8; val_len as usize];
        self.data.read_exact(&mut key)?;
        self.data.read_exact(&mut val)?;
        Ok((KeyValuePair::new(key, val), u64::from(8 + key_len + val_len)))
    }

    pub(crate) fn scan_range(&mut self, key: &ByteStr, start: u64, end: u64) -> io::Result<Option<KeyValuePair>> {
        let mut pos = start;
        while let Some((kv, len)) = self.read_record(pos)? {
            if kv.key_ref() == key {
                return Ok(Some(kv));
            } else {
                pos += len;
                if pos >= end {
                    break;
                }
            }
        }
        Ok(None)
    }

}

impl Read for ReadOnlyDataFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.data.read(buf)
    }
}

impl Seek for ReadOnlyDataFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.data.seek(pos)
    }
}

pub(crate) trait SizedFile {
    fn size(&mut self) -> io::Result<u64>;
}

impl<T> SizedFile for T where T: Seek {
    fn size(&mut self) -> io::Result<u64> {
        let size = self.seek(SeekFrom::End(0));
        self.seek(SeekFrom::Start(0))?;
        size
    }
}
