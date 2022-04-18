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
  let query = format!("SELECT * FROM file_view WHERE share_target IN ({})",
                      questions.join(","));
  let stmt = conn.prep(query).unwrap();
  conn.exec(stmt, sample).unwrap()
}

// Workload execution.
pub fn reads<'a>(
    conn: &mut Conn,
    sample: &Vec<&'a User>,
    expected: &Vec<usize>) -> u128 {
  let questions: Vec<_> = sample.iter().map(|_| "?".to_string()).collect();
  let query = format!("SELECT * FROM file_view WHERE share_target IN ({})",
                      questions.join(","));
  let stmt = conn.prep(query).unwrap();
  let userids = sample.iter().map(|s| &s.uid).collect::<Vec<_>>();

  let now = std::time::Instant::now();
  let r: Vec<usize> = conn.exec(stmt, userids)
                           .unwrap()
                           .into_iter()
                           .map(|v: Row| v.get::<usize, usize>(0).unwrap())
                           .collect();
  let time = now.elapsed().as_micros();

  if expected != &r {
    panic!("Read returns wrong result. Expected: {:?}, found: {:?}", expected, r);
  }
  time
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
  conn.query_drop(&format!("INSERT INTO oc_users VALUES ('{uid}', '{uid}', \
                            '{pw}')",
                           uid = user.uid,
                           pw = ""))
      .unwrap();
}

pub fn insert_group<'a>(conn: &mut Conn, group: &Group<'a>) {
  conn.query_drop(&format!("INSERT INTO oc_groups VALUES ('{}')", &group.gid))
      .unwrap();
  group.users.iter().for_each(|(i, u)| {
                      conn.query_drop(&format!("INSERT INTO oc_group_user \
                                                VALUES ({}, '{}', '{}')",
                                               i, group.gid, u.uid))
                          .unwrap()
                    });
}

pub fn insert_file<'a>(conn: &mut Conn, file: &File<'a>) {
  conn.query_drop(&format!("INSERT INTO oc_files VALUES ({}, '{}')",
                           file.id, file.id))
      .unwrap()
}

pub fn insert_share<'a>(conn: &mut Conn, share: &Share<'a>) {
  let (share_type, user_target, group_target) = match share.share_with {
    ShareType::Direct(u) => (0, quoted(&u.uid), "NULL".to_string()),
    ShareType::Group(g) => (1, "NULL".to_string(), quoted(&g.gid)),
  };
  conn.query_drop(format!("INSERT INTO oc_share VALUES ({share_id}, \
                           {share_type}, {user_target}, {group_target}, \
                           '{owner}', '{initiator}', NULL, 'file', {file}, \
                           '', '', 24, 0, 0, NULL, '', 1, '', 24, 19);",
                          share_id = share.id,
                          user_target = user_target,
                          group_target = group_target,
                          share_type = share_type,
                          owner = share.file.owned_by.uid,
                          initiator = share.file.owned_by.uid,
                          file = share.file.id,))
      .unwrap()
}
