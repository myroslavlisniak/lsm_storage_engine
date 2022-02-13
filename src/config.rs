use config::{ConfigError, Config as Conf, File};
use serde_derive::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub base_path: String,
    pub memtable_limit_bytes: usize,
    pub sstable_level_limit: usize
}
impl Config {
    pub fn new() -> Result<Self, ConfigError> {
        let mut s = Conf::default();
        s.merge(File::with_name("config/default"))?;
        s.try_into()
    }
}