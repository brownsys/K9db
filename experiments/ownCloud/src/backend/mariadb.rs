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
pub fn reads_with_data<'a>(conn: &mut Conn, sample: &Vec<String>) -> Vec<Row> {
  let query = format!(
    "(SELECT s.id as sid, s.item_source, s.share_type, f.fileid, f.path, st.id AS storage_string_id, s.share_with as share_target
      FROM oc_share s
      LEFT JOIN oc_filecache f ON s.file_source = f.fileid
      LEFT JOIN oc_storages st ON f.storage = st.numeric_id
      WHERE (s.share_type = 0) AND s.share_with IN ({userids}))
     UNION 
      (SELECT s.id as sid, s.item_source, s.share_type, f.fileid, f.path, st.id AS storage_string_id, oc_group_user.uid as share_target
      FROM oc_share s
      LEFT JOIN oc_filecache f ON s.file_source = f.fileid
      LEFT JOIN oc_storages st ON f.storage = st.numeric_id
      JOIN oc_group_user ON s.share_with_group = oc_group_user.gid
      WHERE (s.share_type = 1) AND oc_group_user.uid IN ({userids}))",
    userids = sample.join(",")
  );
  conn.query(query).unwrap()
}

// Workload execution.
pub fn reads<'a>(
  conn: &mut Conn,
  sample: &Vec<&'a User>,
  expected: &Vec<usize>,
) -> u128 {
  // Serialize user ids.
  let userids = sample.iter().map(|s| quoted(&s.uid)).collect::<Vec<_>>();

  // Use a direct query.
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

pub fn read_file_pk<'a>(conn: &mut Conn, file: &File<'a>) -> u128 {
  let now = std::time::Instant::now();
  conn
    .query_drop(&format!(
      "SELECT * FROM oc_files WHERE id = {}",
      file.id
    ))
    .unwrap();
  now.elapsed().as_micros()
}

pub fn update_file_pk<'a>(conn: &mut Conn, file: &File<'a>, new_name: String) -> u128 {
  let now = std::time::Instant::now();
  conn
    .query_drop(&format!(
      // "UPDATE oc_files SET file_name = {} WHERE id = {}",
      // new_name, file.id
      "SELECT * FROM oc_files WHERE id = {}",
      file.id
    ))
    .unwrap();
  now.elapsed().as_micros()
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
                           '', '', 24, 0, 0, NULL, '', 1, '', 24, '19');",
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
