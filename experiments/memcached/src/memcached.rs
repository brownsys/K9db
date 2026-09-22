// Ported from memcached/memcached.h.
use memcached::proto::{Operation, ProtoType};
use memcached::Client;

use crate::encode::{encode_key, MemcachedKey, MemcachedRecord};

pub struct MemcachedConnection {
    conn: Client,
}

impl MemcachedConnection {
    pub fn new() -> MemcachedConnection {
        // Connect to Memcached server.
        let conn = Client::connect(&[("tcp://localhost:11211", 1)], ProtoType::Binary)
            .expect("Couldn't add server");
        MemcachedConnection { conn }
    }

    // Writing to a memcached server.
    pub fn write(&mut self, query: u64, key: &MemcachedKey, records: &[MemcachedRecord]) -> u64 {
        let mut size = 0u64;
        for (i, record) in records.iter().enumerate() {
            // Key.
            let complete_key = encode_key(query, key, i as u64);
            let keylen = complete_key.len() as u64;

            // Record.
            let recordlen = record.len() as u64;

            // Put the key/record in memcached.
            size += keylen + recordlen;
            if let Err(status) = self
                .conn
                .add(complete_key.as_bytes(), record.as_bytes(), 0, 0)
            {
                panic!(
                    "Couldn't set row: {:?}\n{}\n{}",
                    status, complete_key, record
                );
            }
        }
        size
    }
}
