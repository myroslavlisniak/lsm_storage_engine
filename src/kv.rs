pub type ByteString = Vec<u8>;
pub type ByteStr = [u8];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyValuePair {
    key: ByteString,
    value: ByteString,
}

impl KeyValuePair {
    pub fn new(key: ByteString, value: ByteString) -> KeyValuePair {
        KeyValuePair { key, value }
    }

    pub fn key_ref(&self) -> &ByteStr {
        &self.key
    }
    pub fn value_ref(&self) -> &ByteStr {
        &self.value
    }

    pub fn key_owned(self) -> ByteString {
        self.key
    }

    pub fn key_cloned(&self) -> ByteString {
        self.key.clone()
    }

    pub fn value_owned(self) -> ByteString {
        self.value
    }
}
