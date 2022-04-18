extern crate mysql;
use mysql::prelude::Queryable;
use mysql::{Conn, OptsBuilder, Row};

// src/models.rs
use super::super::models::{File, Group, Share, ShareType, User};

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

  // Use direct statements.
  let query = format!(
    "SELECT * FROM file_view WHERE share_target IN ({})",
    userids.join(",")
  );
  let now = std::time::Instant::now();
  let rows: Vec<Row> = conn.query(&query).unwrap();
  let prepared_time = now.elapsed().as_micros();

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

  // Use prepared queries.
  let now = std::time::Instant::now();
  let rows = reads_with_data(conn, &userids);
  let direct_time = now.elapsed().as_micros();

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

  if prepared_time < direct_time { prepared_time } else { direct_time }
}

pub fn direct<'a>(conn: &mut Conn, share: &Share<'a>) -> u128 {
  let now = std::time::Instant::now();
  insert_share(conn, share);
  now.elapsed().as_micros()
}

pub fn indirect<'a>(conn: &mut Conn, share: &Share<'a>) -> u128 {
  direct(conn, share)
}

// Inserts.
pub fn insert_user(conn: &mut Conn, user: &User) {
  conn
    .query_drop(&format!(
      "INSERT INTO oc_users VALUES ('{uid}', '{uid}', \
                            '{pw}')",
      uid = user.uid,
      pw = ""
    ))
    .unwrap();
}

pub fn insert_group<'a>(conn: &mut Conn, group: &Group<'a>) {
  conn
    .query_drop(&format!("INSERT INTO oc_groups VALUES ('{}')", &group.gid))
    .unwrap();
  group.users.iter().for_each(|(i, u)| {
    conn
      .query_drop(&format!(
        "INSERT INTO oc_group_user \
                                                VALUES ({}, '{}', '{}')",
        i, group.gid, u.uid
      ))
      .unwrap()
  });
}

pub fn insert_file<'a>(conn: &mut Conn, file: &File<'a>) {
  conn
    .query_drop(&format!(
      "INSERT INTO oc_files VALUES ({}, '{}')",
      file.id, file.id
    ))
    .unwrap()
}

pub fn insert_share<'a>(conn: &mut Conn, share: &Share<'a>) {
  let (share_type, user_target, group_target) = match share.share_with {
    ShareType::Direct(u) => (0, quoted(&u.uid), "NULL".to_string()),
    ShareType::Group(g) => (1, "NULL".to_string(), quoted(&g.gid)),
  };
  conn
    .query_drop(format!(
      "INSERT INTO oc_share VALUES ({share_id}, \
                           {share_type}, {user_target}, {group_target}, \
                           '{owner}', '{initiator}', NULL, 'file', {file}, \
                           '', '', 24, 0, 0, NULL, '', 1, '', 24, 19);",
      share_id = share.id,
      user_target = user_target,
      group_target = group_target,
      share_type = share_type,
      owner = share.file.owned_by.uid,
      initiator = share.file.owned_by.uid,
      file = share.file.id,
    ))
    .unwrap()
}
