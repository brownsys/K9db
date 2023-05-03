-- Free/unsharded table.
CREATE TABLE oc_storages (
  id VARCHAR(64) ,
  numeric_id INT,
  available INT,
  last_checked INT,
  PRIMARY KEY (numeric_id)
);

-- Sharded tables.
CREATE DATA_SUBJECT TABLE oc_users (
  uid VARCHAR(64),
  displayname VARCHAR(64),
  password VARCHAR(255),
  PRIMARY KEY(uid)
);

CREATE TABLE oc_groups (
  gid VARCHAR(64),
  PRIMARY KEY(gid)
);

CREATE TABLE oc_group_user (
  id INT PRIMARY KEY,
  gid VARCHAR(64) OWNS oc_groups(gid),
  uid VARCHAR(64) OWNED_BY oc_users(uid)
);

CREATE TABLE oc_files (
  id INTEGER PRIMARY KEY,
  file_name VARCHAR(255)
);

CREATE TABLE oc_share (
  id INT PRIMARY KEY,
  share_type INT,
  share_with VARCHAR(255) OWNED_BY oc_users(uid),
  share_with_group VARCHAR(255) OWNED_BY oc_groups(gid),
  uid_owner VARCHAR(64) OWNED_BY oc_users(uid),
  uid_initiator VARCHAR(64) REFERENCES oc_users(uid),
  parent INT ,
  item_type VARCHAR(64) ,
  item_source INTEGER OWNS oc_files(id),
  item_target VARCHAR(255),
  file_target VARCHAR(512),
  permissions INT,
  stime INT,
  accepted INT,
  expiration DATETIME ,
  token VARCHAR(32),
  mail_send INT,
  share_name VARCHAR(64),
  file_source INT,
  attributes TEXT
);

CREATE TABLE oc_filecache (
  fileid INTEGER PRIMARY KEY OWNED_BY oc_files(id),
  storage INTEGER REFERENCES oc_storages(numeric_id),
  path VARCHAR(4000),
  path_hash VARCHAR(32),
  parent INT,
  name VARCHAR(250),
  mimetype INTEGER,
  mimepart INTEGER,
  size INT,
  encrypted INTEGER,
  unencrypted_size INT,
  etag VARCHAR(40),
  permissions INTEGER,
  checksum VARCHAR(255),
  mtime INT,
  storage_mtime INT
);

-- -- shoudl also return s.*
CREATE VIEW file_view AS '"(
  SELECT s.id as sid, s.item_source, s.share_type, f.fileid, f.path, st.id AS storage_string_id, s.share_with as share_target
  FROM oc_share s
  LEFT JOIN oc_filecache f ON s.file_source = f.fileid
  LEFT JOIN oc_storages st ON f.storage = st.numeric_id
  WHERE (s.share_type = 0) AND s.share_with = ?
) UNION (
  SELECT s.id as sid, s.item_source, s.share_type, f.fileid, f.path, st.id AS storage_string_id, oc_group_user.uid as share_target
  FROM oc_share s
  LEFT JOIN oc_filecache f ON s.file_source = f.fileid
  LEFT JOIN oc_storages st ON f.storage = st.numeric_id
  JOIN oc_group_user ON s.share_with_group = oc_group_user.gid
  WHERE (s.share_type = 1) AND oc_group_user.uid = ?
)"';

-- CREATE VIEW file_view AS 
-- '"(SELECT s.id as sid, s.item_source, s.share_type, s.share_with as share_target
-- FROM oc_share s
-- WHERE (s.share_type = 0) AND s.share_with = ?)
-- UNION
-- (SELECT s.id as sid, s.item_source, s.share_type, oc_group_user.uid as share_target
-- FROM oc_share s
-- JOIN oc_group_user ON s.share_with_group = oc_group_user.gid
-- WHERE (s.share_type = 1) AND oc_group_user.uid = ?
--    )
-- ORDER BY sid ASC
-- "';
