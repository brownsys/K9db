extern crate mysql;
use mysql::prelude::{FromValue, Queryable};
use mysql::{Conn, OptsBuilder};

extern crate memcached;
use memcached::client::Client;
use memcached::proto::{MultiOperation, NoReplyOperation, Operation};

use std::collections::HashMap;

// src/models.rs
use super::super::models::{File, Group, Share, ShareType, User};
// src/backend/sql.rs
use super::mariadb;

// Encode/decode memcached values.
fn quoted(s: &str) -> String {
  format!("'{}'", s)
}
fn encode_user(u: &str) -> String {
  format!("u{}", u)
}
fn encode_group(g: &str) -> String {
  format!("g{}", g)
}
fn encode_value(value: &mysql::Value) -> String {
  match value {
    mysql::Value::NULL => "NULL".to_string(),
    mysql::Value::Int(i) => format!("{:08}", i),
    mysql::Value::UInt(i) => format!("{:08}", i),
    mysql::Value::Float(i) => format!("{}", i),
    mysql::Value::Double(i) => format!("{}", i),
    mysql::Value::Date(..) => panic!("Unsupported Date type"),
    mysql::Value::Time(..) => panic!("Unsupported Time type"),
    mysql::Value::Bytes(b) => std::str::from_utf8(b).unwrap().to_string(),
  }
}
fn encode_row(row: mysql::Row) -> String {
  row
    .unwrap()
    .iter()
    .map(|v| encode_value(v))
    .collect::<Vec<String>>()
    .join(",")
}
fn decode_row(bytes: &[u8]) -> Vec<usize> {
  let data = std::str::from_utf8(bytes).unwrap();
  let mut ids = Vec::new();
  if data.len() > 0 {
    data.split(";").for_each(|row| {
      let cols: Vec<_> = row.split(",").collect();
      ids.push(cols[0].parse::<usize>().unwrap());
    });
  }
  ids
}

// Warmup cache
pub fn warmup(conn: &mut Conn, client: &mut Client) {
  // Get all the data.
  let rows = mariadb::read_all_with_data(conn);
  
  // Group by user id (share_target)
  let mut map: HashMap<String, String> = HashMap::new();
  for row in rows {
    let key = encode_user(&row.get::<String, usize	>(6).unwrap());
    if let Some(v) = map.get_mut(&key) {
      v.push(';');
      v.push_str(&encode_row(row));
    } else {
      map.insert(key, encode_row(row));
    }
  }

  // Store data in memcached.
  client
    .set_multi(
      map
        .iter()
        .map(|(k, v)| (k.as_bytes(), (v.as_bytes(), 0, 0)))
        .collect(),
    )
    .unwrap();
}


// Workload execution.
pub fn reads(
  conn: &mut Conn,
  client: &mut Client,
  sample: &Vec<User>,
  expected: &Vec<usize>,
) -> u128 {
  let now = std::time::Instant::now();

  // Look up keys in memcached.
  let strs = sample.iter().map(|u| encode_user(&u.uid)).collect::<Vec<_>>();
  let keys = strs.iter().map(|s| s.as_bytes()).collect::<Vec<_>>();
  let result = client.get_multi(&keys).unwrap();

  // Find out which keys were found and which were invalidated.
  let mut misses: Vec<String> = Vec::with_capacity(sample.len() * 10);
  let mut ids = Vec::new();
  for i in 0..keys.len() {
    if let Some((row, _)) = result.get(keys[i]) {
      ids.append(&mut decode_row(row));
    } else {
      misses.push(quoted(&sample[i].uid));
    }
  }

  // Find values for invalidated keys.
  if misses.len() > 0 {
    let rows = mariadb::reads_with_data(conn, &misses);
    let mut map: HashMap<String, String> = HashMap::new();
    for row in rows {
      ids.push(row.get::<usize, usize>(0).unwrap());
      let key = encode_user(&row.get::<String, usize	>(6).unwrap());
      if let Some(v) = map.get_mut(&key) {
        v.push(';');
        v.push_str(&encode_row(row));
      } else {
        map.insert(key, encode_row(row));
      }
    }

    client
      .set_multi(
        map
          .iter()
          .map(|(k, v)| (k.as_bytes(), (v.as_bytes(), 0, 0)))
          .collect(),
      )
      .unwrap();
  }

  // Validate output.
  let time = now.elapsed().as_micros();
  ids.sort();
  if expected != &ids {
    panic!(
      "Read returns wrong result. Expected: {:?}, found: {:?}",
      expected, ids
    );
  }

  time
}

pub fn direct(
  conn: &mut Conn,
  client: &mut Client,
  share: &Share,
) -> u128 {
  let now = std::time::Instant::now();
  // Write to database.
  mariadb::direct(conn, share);

  // Invalidate.
  if let ShareType::Direct(u) = &share.share_with {
    // Can invalidate user directly.
    let key = encode_user(&u.uid);
    client.delete(key.as_bytes());

    // Done.
    now.elapsed().as_micros()
  } else {
    panic!("indirect called with direct!");
  }
}

pub fn indirect(
  conn: &mut Conn,
  client: &mut Client,
  share: &Share,
) -> u128 {
  let now = std::time::Instant::now();
  // Write to database.
  mariadb::indirect(conn, share);

  // Invalidate.
  if let ShareType::Group(g) = &share.share_with {
    // Read group members.
    let key = encode_group(&g.gid);
    let (value, _) = client.get(key.as_bytes()).unwrap();
    let strval = String::from_utf8(value).unwrap();
    let users = strval.split(",").map(encode_user).collect::<Vec<_>>();

    // Invalidate group members.
    let keys = users.iter().map(|u| u.as_bytes()).collect::<Vec<_>>();
    client.delete_multi(&keys);

    // Done.
    now.elapsed().as_micros()
  } else {
    panic!("indirect called with direct!");
  }
}

pub fn read_file_pk(conn: &mut Conn, files: &Vec<File>) -> u128 {
  mariadb::read_file_pk(conn, files)
}

pub fn update_file_pk(conn: &mut Conn, file: &File, new_name: String) -> u128 {
  mariadb::update_file_pk(conn, file, new_name)
}

// Inserts (Priming).
pub fn insert_users(conn: &mut Conn, client: &mut Client, users: Vec<User>) {
  mariadb::insert_users(conn, users);
}
pub fn insert_groups(conn: &mut Conn, client: &mut Client, groups: Vec<Group>) {
  // Write membership to memcached.
  let mut map: HashMap<String, String> = HashMap::new();
  for group in &groups {
    let key = encode_group(&group.gid);
    let mut users = String::new();
    for (_, uid) in &group.users {
      if users.len() > 0 {
        users.push(',');
      }
      users.push_str(&uid);
    }
    map.insert(key, users);
  }

  client
    .set_multi(
      map
        .iter()
        .map(|(k, v)| (k.as_bytes(), (v.as_bytes(), 0, 0)))
        .collect(),
    )
    .unwrap();

  // Write to database.
  mariadb::insert_groups(conn, groups);
}
pub fn insert_files(conn: &mut Conn, client: &mut Client, files: Vec<File>) {
  mariadb::insert_files(conn, files)
}
pub fn insert_shares(conn: &mut Conn, client: &mut Client, shares: Vec<Share>) {
  mariadb::insert_shares(conn, shares)
}
