extern crate mysql;
use std::io::Write;
use mysql::prelude::Queryable;
use mysql::{Conn, OptsBuilder, Row, LocalInfileHandler};

// src/models.rs
use super::super::models::{File, Group, Share, ShareType, User};

// Helper.
fn quoted(s: &str) -> String {
  format!("'{}'", s)
}

// Helper: gets the results not just the time.
pub fn reads_with_data(conn: &mut Conn, sample: &Vec<String>) -> Vec<Row> {
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

pub fn read_all_with_data(conn: &mut Conn) -> Vec<Row> {
  let query = format!(
    "(SELECT s.id as sid, s.item_source, s.share_type, f.fileid, f.path, st.id AS storage_string_id, s.share_with as share_target
      FROM oc_share s
      LEFT JOIN oc_filecache f ON s.file_source = f.fileid
      LEFT JOIN oc_storages st ON f.storage = st.numeric_id
      WHERE (s.share_type = 0))
     UNION 
      (SELECT s.id as sid, s.item_source, s.share_type, f.fileid, f.path, st.id AS storage_string_id, oc_group_user.uid as share_target
      FROM oc_share s
      LEFT JOIN oc_filecache f ON s.file_source = f.fileid
      LEFT JOIN oc_storages st ON f.storage = st.numeric_id
      JOIN oc_group_user ON s.share_with_group = oc_group_user.gid
      WHERE (s.share_type = 1))",
  );
  conn.query(query).unwrap()
}

// Workload execution.
pub fn reads(
  conn: &mut Conn,
  sample: &Vec<User>,
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

pub fn direct(conn: &mut Conn, shares: &Vec<Share>) -> u128 {
  let now = std::time::Instant::now();
  for share in shares {
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
  now.elapsed().as_micros()
}

pub fn indirect(conn: &mut Conn, shares: &Vec<Share>) -> u128 {
  direct(conn, shares)
}

pub fn read_file_pk(conn: &mut Conn, files: &Vec<File>) -> u128 {
  let fids = files.iter().map(|f| format!("{}", &f.id)).collect::<Vec<_>>();

  let now = std::time::Instant::now();
  conn
    .query_drop(&format!(
      "SELECT * FROM oc_files WHERE id IN ({})",
      fids.join(",")
    ))
    .unwrap();
  now.elapsed().as_micros()
}
pub fn update_file_pk(conn: &mut Conn, file: &File, new_name: String) -> u128 {
  let now = std::time::Instant::now();
  conn
    .query_drop(&format!(
      "UPDATE oc_files SET file_name = '{}' WHERE id = {}",
      new_name.to_string(), file.id
      // "SELECT * FROM oc_files WHERE id = {}",
      // file.id
    ))
    .unwrap();
  now.elapsed().as_micros()
}

// Inserts (Priming).
pub fn insert_users(conn: &mut Conn, users: Vec<User>) {
  conn.set_local_infile_handler(Some(LocalInfileHandler::new(move |_, stream| {
    for user in &users {
      write!(stream, "{uid},{uid},{pw}\n", uid = user.uid, pw = "")?;
    }
    Ok(())
  })));
  conn.query_drop("SET rocksdb_bulk_load_allow_unsorted = 1").unwrap();
  conn.query_drop("SET rocksdb_bulk_load = 1").unwrap();
  conn.query_drop("LOAD DATA LOCAL INFILE 'file' INTO TABLE oc_users FIELDS TERMINATED BY ','").unwrap();
  conn.query_drop("SET rocksdb_bulk_load = 0").unwrap();
}

pub fn insert_groups(conn: &mut Conn, groups: Vec<Group>) {
  conn.set_local_infile_handler(Some(LocalInfileHandler::new(move |name, stream| {
    if name == b"gfile" {
      for group in &groups {
        write!(stream, "{gid}\n", gid = group.gid)?;
      }
    } else {
      for group in &groups {
        group.users.iter().for_each(|(i, uid)| {
          write!(stream, "{i},{gid},{uid}\n", i = i, gid = group.gid, uid = uid).unwrap();
        });
      }
    }
    Ok(())
  })));
  conn.query_drop("SET rocksdb_bulk_load = 1").unwrap();
  conn.query_drop("LOAD DATA LOCAL INFILE 'gfile' INTO TABLE oc_groups FIELDS TERMINATED BY ','").unwrap();
  conn.query_drop("LOAD DATA LOCAL INFILE 'afile' INTO TABLE oc_group_user FIELDS TERMINATED BY ','").unwrap();
    conn.query_drop("SET rocksdb_bulk_load = 0").unwrap();
}

pub fn insert_files(conn: &mut Conn, files: Vec<File>) {
  conn.set_local_infile_handler(Some(LocalInfileHandler::new(move |_, stream| {
    for file in &files {
      write!(stream, "{fid},{fid}\n", fid = file.id)?;
    }
    Ok(())
  })));
  conn.query_drop("SET rocksdb_bulk_load = 1").unwrap();
  conn.query_drop("LOAD DATA LOCAL INFILE 'file' INTO TABLE oc_files FIELDS TERMINATED BY ','").unwrap();
  conn.query_drop("SET rocksdb_bulk_load = 0").unwrap();
}

pub fn insert_shares(conn: &mut Conn, shares: Vec<Share>) {
  conn.set_local_infile_handler(Some(LocalInfileHandler::new(move |_, stream| {
    for share in &shares {
      match &share.share_with {
        ShareType::Direct(u) => 
          write!(stream,
                "{id},0,{uid},\\N,{oid},{oid},\\N,file,{fid},,24,0,0,\\N,,1,,24,19\n",
                 id = share.id,
                 uid = u.uid,
                 oid = share.file.owned_by,
                 fid = share.file.id),
        ShareType::Group(g) => 
          write!(stream,
                "{id},1,\\N,{gid},{oid},{oid},\\N,file,{fid},,24,0,0,\\N,,1,,24,19\n",
                 id = share.id,
                 gid = g.gid,
                 oid = share.file.owned_by,
                 fid = share.file.id),
      };
    }
    Ok(())
  })));
  conn.query_drop("SET rocksdb_bulk_load = 1").unwrap();
  conn.query_drop("LOAD DATA LOCAL INFILE 'file' INTO TABLE oc_share FIELDS TERMINATED BY ','").unwrap();
  conn.query_drop("SET rocksdb_bulk_load = 0").unwrap();
}
