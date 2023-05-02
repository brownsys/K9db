// mysql connector.
extern crate mysql;
use mysql::prelude::Queryable;
use mysql::{Conn, OptsBuilder};

// Memcached connector.
extern crate memcached;
use memcached::client::Client;
use memcached::proto::ProtoType;

// Connection properties.
const DB_USER: &'static str = "k9db";
const DB_PASSWORD: &'static str = "password";
const DB_NAME: &'static str = "owncloud";

// Schemas.
const PELTON_SCHEMA: &'static str = include_str!("../data/schema.sql");
const MARIADB_SCHEMA: &'static str = include_str!("../data/mysql-schema.sql");

pub fn pelton_connect(ip: &str) -> Conn {
  // Start a connection.
  let opts = OptsBuilder::new()
    .user(Some(DB_USER))
    .pass(Some(DB_PASSWORD))
    .tcp_port(10001)
    .ip_or_hostname(Some(ip))
    .tcp_nodelay(true);
  let mut connection = Conn::new(opts).unwrap();
  // Create the schema.
  run_file(&mut connection, PELTON_SCHEMA);
  connection
}

pub fn mariadb_connect(ip: &str) -> Conn {
  // Start a connection.
  let opts = OptsBuilder::new()
    .user(Some(DB_USER))
    .pass(Some(DB_PASSWORD))
    .ip_or_hostname(Some(ip));
  let mut connection = Conn::new(opts).unwrap();
  // Clean up database.
  connection
    .query_drop(format!("DROP DATABASE IF EXISTS {};", DB_NAME))
    .unwrap();
  connection
    .query_drop(format!("CREATE DATABASE {};", DB_NAME))
    .unwrap();
  connection.query_drop(format!("USE {}", DB_NAME)).unwrap();
  // Create the schema.
  run_file(&mut connection, MARIADB_SCHEMA);
  connection
}

pub fn memcached_connect() -> Client {
  return Client::connect(&[("tcp://127.0.0.1:11211", 1)], ProtoType::Binary)
    .unwrap();
}

fn run_file<Q: Queryable>(conn: &mut Q, file: &str) {
  let statements = file.lines().filter(|l| !l.trim_start().starts_with("--"));
  let statements = statements.fold(String::new(), |s, l| s + " " + l);
  let statements = statements.split(';');
  let statements = statements.filter(|q| !q.chars().all(&char::is_whitespace));
  for statement in statements {
    conn.query_drop(&statement.trim_start()).unwrap();
  }
}
