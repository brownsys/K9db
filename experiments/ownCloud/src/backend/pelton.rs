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
pub fn reads_with_data(conn: &mut Conn, sample: &Vec<&str>) -> Vec<Row> {
  let questions: Vec<_> = sample.iter().map(|_| "?".to_string()).collect();
  let query = format!(
    "SELECT * FROM file_view WHERE share_target IN ({})",
    questions.join(",")
  );
  let stmt = conn.prep(query).unwrap();
  conn.exec(stmt, sample).unwrap()
}

// Load:
pub fn reads(
  conn: &mut Conn,
  sample: &Vec<User>,
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
pub fn direct(conn: &mut Conn, shares: &Vec<Share>) -> u128 {
  mariadb::direct(conn, shares)
}

pub fn indirect(conn: &mut Conn, shares: &Vec<Share>) -> u128 {
  mariadb::indirect(conn, shares)
}
pub fn read_file_pk(conn: &mut Conn, files: &Vec<File>) -> u128 {
  mariadb::read_file_pk(conn, files)
}
pub fn update_file_pk(conn: &mut Conn, file: &File, new_name: String) -> u128 {
  mariadb::update_file_pk(conn, file, new_name)
}


// Inserts / Priming.
pub fn insert_users(conn: &mut Conn, users: Vec<User>) {
  for user in &users {
    conn
      .query_drop(&format!(
        "INSERT INTO oc_users VALUES ('{uid}', '{uid}', '{pw}')",
        uid = user.uid,
        pw = ""
      ))
      .unwrap();
  }
}

pub fn insert_groups(conn: &mut Conn, groups: Vec<Group>) {
  for group in &groups {
    conn
      .query_drop(&format!("INSERT INTO oc_groups VALUES ('{}')", &group.gid))
      .unwrap();
    group.users.iter().for_each(|(i, uid)| {
      conn
        .query_drop(&format!(
          "INSERT INTO oc_group_user VALUES ({}, '{}', '{}')",
          i, group.gid, uid
        ))
        .unwrap();
    });
  }
}

pub fn insert_files(conn: &mut Conn, files: Vec<File>) {
  for file in &files {
    conn
      .query_drop(&format!(
        "INSERT INTO oc_files VALUES ({}, '{}')",
        file.id, file.id
      ))
      .unwrap();
  }
}

pub fn insert_shares(conn: &mut Conn, shares: Vec<Share>) {
  for share in &shares {
    let (share_type, user_target, group_target) = match &share.share_with {
    ShareType::Direct(u) => (0, quoted(&u.uid), "NULL".to_string()),
    ShareType::Group(g) => (1, "NULL".to_string(), quoted(&g.gid)),
  };
  conn
    .query_drop(format!(
      "INSERT INTO oc_share VALUES ({share_id}, \
                           {share_type}, {user_target}, {group_target}, \
                           '{owner}', '{initiator}', NULL, 'file', {file}, \
                           '', '', 24, 0, 0, NULL, '', 1, '', 24, '19');",
      share_id = share.id,
      user_target = user_target,
      group_target = group_target,
      share_type = share_type,
      owner = share.file.owned_by,
      initiator = share.file.owned_by,
      file = share.file.id,
    ))
    .unwrap();
  }
}
