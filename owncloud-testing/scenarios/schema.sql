
CREATE TABLE oc_storages (
  id VARCHAR(64) ,
  numeric_id INT NOT NULL,
  available INT NOT NULL,
  last_checked INT,
  PRIMARY KEY (numeric_id)
);
CREATE TABLE oc_filecache (
  fileid INTEGER PRIMARY KEY NOT NULL,
  storage INTEGER NOT NULL REFERENCES oc_storages(numeric_id),
  path VARCHAR(4000),
  path_hash VARCHAR(32) NOT NULL,
  parent INT NOT NULL,
  name VARCHAR(250),
  mimetype INTEGER NOT NULL,
  mimepart INTEGER NOT NULL,
  size INT NOT NULL,
  encrypted INTEGER NOT NULL,
  unencrypted_size INT NOT NULL,
  etag VARCHAR(40),
  permissions INTEGER,
  checksum VARCHAR(255),
  mtime INT NOT NULL,
  storage_mtime INT NOT NULL
);
CREATE TABLE oc_files (
  id INTEGER PRIMARY KEY NOT NULL,
  file_name VARCHAR(255)
);

CREATE TABLE  oc_users (
  uid VARCHAR(64) NOT NULL,
  PII_displayname VARCHAR(64),
  password VARCHAR(255) NOT NULL,
  PRIMARY KEY(uid)
);

CREATE TABLE oc_share (
  id INT NOT NULL,
  share_type INT NOT NULL,
  OWNER_share_with VARCHAR(255) REFERENCES oc_users(uid),
  --share_with_group VARCHAR(255) REFERENCE oc_groups(gid),
  OWNER_uid_owner VARCHAR(64) NOT NULL REFERENCES oc_users(uid),
  uid_initiator VARCHAR(64) REFERENCES oc_users(uid),
  parent INT ,
  item_type VARCHAR(64) NOT NULL ,
  OWNING_item_source INTEGER NOT NULL REFERENCES oc_files(id),
  item_target VARCHAR(255),
  file_target VARCHAR(512),
  permissions INT NOT NULL,
  stime INT NOT NULL,
  accepted INT NOT NULL,
  expiration DATETIME ,
  token VARCHAR(32),
  mail_send INT NOT NULL,
  share_name VARCHAR(64),
  file_source INT,
  attributes TEXT
);