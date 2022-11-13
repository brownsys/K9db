CREATE TABLE oc_storages (
  id VARCHAR(64) ,
  numeric_id INT,
  available INT,
  last_checked INT,
  PRIMARY KEY (numeric_id)
);

CREATE TABLE oc_users (
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
  gid VARCHAR(64) REFERENCES oc_groups(gid),
  uid VARCHAR(64) REFERENCES oc_users(uid)
);

CREATE TABLE oc_files (
  id INTEGER PRIMARY KEY,
  file_name VARCHAR(255)
);

CREATE TABLE oc_share (
  id INT PRIMARY KEY,
  share_type INT,
  share_with VARCHAR(255) REFERENCES oc_users(uid),
  share_with_group VARCHAR(255) REFERENCES oc_groups(gid),
  uid_owner VARCHAR(64) REFERENCES oc_users(uid),
  uid_initiator VARCHAR(64) REFERENCES oc_users(uid),
  parent INT ,
  item_type VARCHAR(64) ,
  item_source INTEGER REFERENCES oc_files(id),
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
  fileid INTEGER PRIMARY KEY REFERENCES oc_files(id),
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

CREATE INDEX oc_share_type_direct ON oc_share(share_with, share_type);
CREATE INDEX oc_share_type_indirect ON oc_share(share_with_group, share_type);
CREATE INDEX oc_group_user_uid ON oc_group_user(uid);
CREATE INDEX oc_storages_numeric_id ON oc_storages(numeric_id);
