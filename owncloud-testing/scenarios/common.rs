extern crate mysql;
pub extern crate chrono;

pub use self::mysql::*;
pub use self::mysql::prelude::*;

const SCHEMA : &'static str = include_str!("schema.sql");

pub fn run_queries_in_str<Q:Queryable>(conn: &mut Q, file: &str) {
    let qs = file.lines().filter(|l| !l.trim_start().starts_with("--")).collect::<String>();
    for q in qs.split(';').filter(|q| !q.chars().all(&char::is_whitespace)) {
        conn.query_drop(&q).unwrap();
    }
}

pub fn prepare_database() -> Conn {
    let opts = OptsBuilder::new().tcp_port(10001);
    let mut c = Conn::new(opts).unwrap();
    run_queries_in_str(&mut c, SCHEMA);
    c
}
