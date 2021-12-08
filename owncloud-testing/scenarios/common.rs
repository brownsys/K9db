extern crate mysql;

pub use self::mysql::*;
pub use self::mysql::prelude::*;

const SCHEMA : &'static str = include_str!("schema.sql");

pub fn run_queries_in_str<Q:Queryable>(conn: &mut Q, file: &str) {
    let qs = file.lines().filter(|l| !l.trim_start().starts_with("--")).fold(String::new(), |mut s, l| { if !s.is_empty() { s.push_str(" "); } s.push_str(l); s });
    for q in qs.split(';').filter(|q| !q.chars().all(&char::is_whitespace)) {
        conn.query_drop(&q.trim_start()).unwrap();
    }
}

pub enum Backend {
    Pelton,
    MySQL,
}

pub fn prepare_database(prepare_and_insert: bool) -> Conn {
    prepare_backend(Backend::Pelton, prepare_and_insert)
}

pub fn prepare_backend(backend: Backend, prepare_and_insert: bool) -> Conn {
    let opts0 = OptsBuilder::new();
    let opts = match backend {
        Backend::Pelton => opts0.tcp_port(10001),
        Backend::MySQL => opts0,
    };
    let mut c = Conn::new(opts).unwrap();
    if prepare_and_insert {
        run_queries_in_str(&mut c, SCHEMA);
    }
    c
}
