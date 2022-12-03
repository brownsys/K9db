extern crate mysql;
use mysql::prelude::Queryable;
use mysql::{Conn, OptsBuilder, Row};

// src/models.rs
use super::super::models::{File, Group, Share, ShareType, User};

// src/backend/mariadb.rs
use super::mariadb;

// Helper.
fn quoted(s: &str) -> String {
  format!("'{}'", s)
}

// Helper: gets the results not just the time.
pub fn reads_with_data<'a>(conn: &mut Conn, sample: &Vec<&'a str>) -> Vec<Row> {
  let questions: Vec<_> = sample.iter().map(|_| "?".to_string()).collect();
  let query = format!(
    "SELECT * FROM file_view WHERE share_target IN ({})",
    questions.join(",")
  );
  let stmt = conn.prep(query).unwrap();
  conn.exec(stmt, sample).unwrap()
}

// Workload execution.
pub fn reads<'a>(
  conn: &mut Conn,
  sample: &Vec<&'a User>,
  expected: &Vec<usize>,
) -> u128 {
  // Serialize user ids.
  let userids = sample.iter().map(|s| &s.uid[..]).collect::<Vec<&str>>();

  // Use prepared queries.
  let now = std::time::Instant::now();
  let rows = reads_with_data(conn, &userids);
  let time = now.elapsed().as_micros();

  // Check result correct.
  let mut results: Vec<_> =
    rows.iter().map(|r| r.get::<usize, usize>(0).unwrap()).collect();
  results.sort();
  if expected != &results {
    panic!(
      "Read returns wrong result. Expected: {:?}, found: {:?}",
      expected, results
    );
  }

  time
}

pub fn direct<'a>(conn: &mut Conn, share: &Share<'a>) -> u128 {
  mariadb::direct(conn, share)
}

pub fn indirect<'a>(conn: &mut Conn, share: &Share<'a>) -> u128 {
  mariadb::indirect(conn, share)
}

// Inserts.
pub fn insert_user(conn: &mut Conn, user: &User) {
  mariadb::insert_user(conn, user)
}

pub fn insert_group<'a>(conn: &mut Conn, group: &Group<'a>) {
  mariadb::insert_group(conn, group)
}

pub fn insert_file<'a>(conn: &mut Conn, file: &File<'a>) {
  mariadb::insert_file(conn, file)
}

pub fn insert_share<'a>(conn: &mut Conn, share: &Share<'a>) {
  mariadb::insert_share(conn, share)
}

// Add update

pub fn read_file_pk<'a>(conn: &mut Conn, files: &Vec<&'a File<'a>>) -> u128 {
  mariadb::read_file_pk(conn, files)
}

pub fn update_file_pk<'a>(conn: &mut Conn, file: &File<'a>, new_name: String) -> u128 {
  mariadb::update_file_pk(conn, file, new_name)
}
