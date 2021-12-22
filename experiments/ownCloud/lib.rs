extern crate mysql;

extern crate memcached_ffi;

pub use self::mysql::*;
pub use self::mysql::prelude::*;
use std::str::FromStr;

pub use memcached_ffi::memcached;

const SCHEMA : &'static str = include_str!("schema.sql");
const MYSQL_SCHEMA : &'static str = include_str!("mysql-schema.sql");

pub fn run_queries_in_str<Q:Queryable>(conn: &mut Q, file: &str) {
    let qs = file.lines().filter(|l| !l.trim_start().starts_with("--")).fold(String::new(), |mut s, l| { if !s.is_empty() { s.push_str(" "); } s.push_str(l); s });
    for q in qs.split(';').filter(|q| !q.chars().all(&char::is_whitespace)) {
        conn.query_drop(&q.trim_start()).unwrap();
    }
}

#[derive(PartialEq, Eq, Debug)]
pub enum Backend {
    Pelton,
    MySQL,
}

impl FromStr for Backend {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "pelton" => Ok(Backend::Pelton),
            "mysql" => Ok(Backend::MySQL),
            _ => Err(format!("Unknown backend {}", s)),
        }
    }
}

impl std::fmt::Display for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            match self {
                Backend::Pelton => "pelton",
                Backend::MySQL => "mysql",
            }
        )
    }
}

pub const DB_NAME : &'static str = "owncloud_test";
pub const DB_USER : &'static str = "root";
pub const DB_PASSWORD : &'static str = "password";

impl Backend {
    pub fn is_mysql(&self) -> bool {
        self == &Backend::MySQL
    }
    pub fn is_pelton(&self) -> bool {
        self == &Backend::Pelton
    }
    pub fn prepare(&self, prepare_and_insert: bool) -> Conn {
        let opts0 = OptsBuilder::new();
        let opts = match self {
            Backend::Pelton => opts0.tcp_port(10001),
            Backend::MySQL => opts0.user(Some(DB_USER)).pass(Some(DB_PASSWORD)),
        };
        let mut c = Conn::new(opts).unwrap();
        if self.is_mysql() {
            c.query_drop(format!("DROP DATABASE IF EXISTS {};", DB_NAME)).unwrap();
            c.query_drop(format!("CREATE DATABASE {};", DB_NAME)).unwrap();
            c.query_drop(format!("USE {}", DB_NAME)).unwrap();
        }
        if prepare_and_insert {
            let schema = match self {
                Backend::Pelton => SCHEMA,
                Backend::MySQL => MYSQL_SCHEMA,
            };
            if self.is_pelton() {
                run_queries_in_str(&mut c, schema);
            } else {
                let mut t = c.start_transaction(TxOpts::default()).unwrap();
                run_queries_in_str(&mut t, schema);
                t.commit().unwrap();
            }
        }
        c
    }
}


pub fn pp_pelton_database() {

    use std::collections::HashSet;
    println!("Dumping database");

    let mut conn = Conn::new(OptsBuilder::new().db_name(Some("pelton")).user(Some("root")).pass(Some("password"))).unwrap();
    let tables : Vec<String> = conn.query("show tables;").unwrap();
    let (user_hashes, mut table_names) : (HashSet<_>, HashSet<_>) = tables.iter().filter_map(|s| s.find('_').map(|i| (&s[..i], &s[(i + 1)..]))).filter(|t| t.0 != "default").unzip();
    let username_marker_table = "username_marker_uid";
    let username_marker_exists = table_names.remove(username_marker_table);
    for user_hash in user_hashes.iter() {
        let user = if username_marker_exists {
            conn.query_first(format!("SELECT * FROM {}_{};", user_hash, username_marker_table)).unwrap().expect("Empty username marker table")
        } else {
            user_hash.to_string()
        };
        println!("=============== μDB for {} ===============", user);
        for tab in table_names.iter() {
            if tab == &username_marker_table { continue; }
            use std::process::Command;
            use std::io::Write;
            println!("TABLE {}", tab);
            std::io::stdout().write_all(
                &Command::new("mysql").args(&["-u", "root", "--password=password", "-B", "pelton", "-e", &format!("SELECT * FROM {}_{};", user_hash, tab)]).output().unwrap().stdout,
            ).unwrap();
        }
        println!("");
    }
}