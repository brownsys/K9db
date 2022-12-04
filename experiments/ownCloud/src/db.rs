// mysql connector.
extern crate mysql;
use mysql::prelude::Queryable;
use mysql::{Conn, OptsBuilder};

// Memcached connector.
extern crate memcached;
use memcached::client::Client;
use memcached::proto::ProtoType;

// Connection properties.
const DB_USER: &'static str = "pelton";
const DB_PASSWORD: &'static str = "password";
const DB_NAME: &'static str = "owncloud";

// Schemas.
const PELTON_SCHEMA: &'static str = include_str!("../data/schema.sql");
const ALL_OWNERS_SCHEMA: &'static str = include_str!("../data/schema_owners.sql");
const MARIADB_SCHEMA: &'static str = include_str!("../data/mysql-schema.sql");

pub fn pelton_connect(views: bool, accessors: bool, echo: bool) -> Conn {
  // Start a connection.
  let opts = OptsBuilder::new()
    .user(Some(DB_USER))
    .pass(Some(DB_PASSWORD))
    .tcp_port(10001)
    .ip_or_hostname(Some("127.0.0.1"))
    .tcp_nodelay(true);
  let mut connection = Conn::new(opts).unwrap();
  if echo {
    connection.query_drop("SET echo").unwrap();
  }
  if views {
    println!("Disabling views...");
    connection.query_drop("SET VIEWS OFF").unwrap();
  }
  // Create the schema.
  if accessors {
    run_file(&mut connection, ALL_OWNERS_SCHEMA);
  } else {
    run_file(&mut connection, PELTON_SCHEMA);
  }
  connection
}

pub fn mariadb_connect() -> Conn {
  let db_ip_result = match std::env::var("LOCAL_IP") {
    Ok(v) => if v == "" { "localhost".to_string() } else { v },
    Err(_) => "localhost".to_string(),
  };
  // Start a connection.
  let opts = OptsBuilder::new()
    .user(Some(DB_USER))
    .pass(Some(DB_PASSWORD))
    .ip_or_hostname(Some(db_ip_result));
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
