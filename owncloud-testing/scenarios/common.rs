extern crate mysql;
pub extern crate chrono;

pub use self::mysql::*;
pub use self::mysql::prelude::*;

const SCHEMA : &'static str = include_str!("schema.sql");

pub fn prepare_database() -> Conn {
    let mut opts = OptsBuilder::new();
    let mut c = Conn::new(opts).unwrap();
    c.query_drop(SCHEMA).unwrap();
    c
}
