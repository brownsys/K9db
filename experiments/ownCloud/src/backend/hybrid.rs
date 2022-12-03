extern crate mysql;
use mysql::prelude::{FromValue, Queryable};
use mysql::{Conn, OptsBuilder};

extern crate memcached;
use memcached::client::Client;
use memcached::proto::{MultiOperation, NoReplyOperation, Operation};

use std::collections::BTreeMap;

// src/models.rs
use super::super::models::{File, Group, Share, ShareType, User};
// src/backend/sql.rs
use super::mariadb;

// Encode/decode memcached values.
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
fn concat_rows(l: &str, r: &str) -> String {
  format!("{};{}", l, r)
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

// Workload execution.
pub fn reads<'a>(
  conn: &mut Conn,
  client: &mut Client,
  sample: &Vec<&'a User>,
  expected: &Vec<usize>,
) -> u128 {
  let now = std::time::Instant::now();

  // Look up keys in memcached.
  let strs = sample
    .iter()
    .map(|u| encode_user(&u.uid))
    .collect::<Vec<_>>();
  let keys = strs.iter().map(|s| s.as_bytes()).collect::<Vec<_>>();
  let result = client.get_multi(&keys).unwrap();

  // Find out which keys were found and which were invalidated.
  let mut misses: Vec<String> = Vec::with_capacity(sample.len());
  let mut ids = Vec::new();
  for i in 0..keys.len() {
    if let Some((row, _)) = result.get(keys[i]) {
      ids.append(&mut decode_row(row));
    } else {
      misses.push(sample[i].uid.clone());
    }
  }

  // Find values for invalidated keys.
  if misses.len() > 0 {
    // Get result from DB.
    let results = mariadb::reads_with_data(conn, &misses);
    let mut map: BTreeMap<String, String> = BTreeMap::new();
    misses.iter().for_each(|uid| {
      map.insert(encode_user(uid), "".to_string());
    });
    for row in results {
      // The user id is the last (sixth) column.
      ids.push(row.get::<usize, usize>(0).unwrap());
      let key = encode_user(&row.get::<String, usize>(6).unwrap());
      // The row is encoded as a string.
      let mut value = encode_row(row);
      // Concat encoding of this row to any previous rows associated with
      // the same user.
      if map.contains_key(&key) {
        let old = map.get(&key).unwrap();
        if old.len() > 0 {
          value = concat_rows(old, &value);
        }
      }
      map.insert(key, value);
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

pub fn direct<'a>(
  conn: &mut Conn,
  client: &mut Client,
  share: &Share<'a>,
) -> u128 {
  let now = std::time::Instant::now();
  // Write to database.
  mariadb::direct(conn, share);

  // Invalidate.
  if let ShareType::Direct(u) = share.share_with {
    // Can invalidate user directly.
    let key = encode_user(&u.uid);
    client.delete(key.as_bytes());

    // Done.
    now.elapsed().as_micros()
  } else {
    panic!("indirect called with direct!");
  }
}

pub fn indirect<'a>(
  conn: &mut Conn,
  client: &mut Client,
  share: &Share<'a>,
) -> u128 {
  let now = std::time::Instant::now();
  // Write to database.
  mariadb::indirect(conn, share);

  // Invalidate.
  if let ShareType::Group(g) = share.share_with {
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

// Inserts.
pub fn insert_user(conn: &mut Conn, client: &mut Client, user: &User) {
  mariadb::insert_user(conn, user);
}

pub fn insert_group<'a>(
  conn: &mut Conn,
  client: &mut Client,
  group: &Group<'a>,
) {
  // Write to database.
  mariadb::insert_group(conn, group);

  // Write membership to memcached.
  let key = encode_group(&group.gid);
  let mut users = String::new();
  for (_, user) in &group.users {
    if users.len() > 0 {
      users.push_str(",");
    }
    users.push_str(&user.uid);
  }
  client.add(key.as_bytes(), users.as_bytes(), 0, 0).unwrap();
}

pub fn insert_file<'a>(conn: &mut Conn, client: &mut Client, file: &File<'a>) {
  mariadb::insert_file(conn, file)
}

pub fn insert_share<'a>(
  conn: &mut Conn,
  client: &mut Client,
  share: &Share<'a>,
) {
  mariadb::insert_share(conn, share)
}

pub fn read_file_pk<'a>(conn: &mut Conn, files: &Vec<&'a File<'a>>) -> u128 {
  mariadb::read_file_pk(conn, files)
}

pub fn update_file_pk<'a>(conn: &mut Conn, file: &File<'a>, new_name: String) -> u128 {
  mariadb::update_file_pk(conn, file, new_name)
}
