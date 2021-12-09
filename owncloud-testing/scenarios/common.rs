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
                &Command::new("mysql").args(["-u", "root", "--password=password", "-B", "pelton", "-e", &format!("SELECT * FROM {}_{};", user_hash, tab)]).output().unwrap().stdout,
            ).unwrap();
        }
        println!("");
    }
}