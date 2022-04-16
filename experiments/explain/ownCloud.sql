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


CREATE TABLE oc_groups (
  gid VARCHAR(64) NOT NULL,
  PRIMARY KEY(gid)
);

CREATE TABLE oc_group_user (
  id INT PRIMARY KEY,
  OWNING_gid VARCHAR(64) NOT NULL REFERENCES oc_groups(gid),
  uid VARCHAR(64) NOT NULL REFERENCES oc_users(uid)
  --UNIQUE group_user_uniq(OWNED_gid, uid)
);

CREATE VIEW users_for_group AS 
'"SELECT oc_group_user.OWNING_gid, oc_group_user.uid, COUNT(*)
FROM oc_group_user 
WHERE oc_group_user.OWNING_gid = ?
GROUP BY (oc_group_user.OWNING_gid, oc_group_user.uid)
HAVING COUNT(*) > 0"' ;

CREATE TABLE oc_share (
  id INT NOT NULL PRIMARY KEY,
  share_type INT NOT NULL,
  ACCESSOR_share_with VARCHAR(255) REFERENCES oc_users(uid),
  OWNER_share_with_group VARCHAR(255) REFERENCES oc_groups(gid),
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

SET echo;


-- CREATE VIEW users_for_file_via_group AS 
-- '"SELECT oc_share.OWNING_item_source, oc_group_user.uid, COUNT(*)
-- FROM oc_share
-- JOIN oc_group_user ON oc_share.OWNER_share_with_group = oc_group_user.OWNING_gid
-- WHERE oc_share.OWNING_item_source = ?
-- GROUP BY (oc_share.OWNING_item_source, oc_group_user.uid)"';


-- CREATE VIEW file_view AS 
-- '"(SELECT s.id as sid, s.OWNING_item_source, s.share_type, s.ACCESSOR_share_with as share_target
-- FROM oc_share s
-- WHERE (s.share_type = 0) AND s.ACCESSOR_share_with = ?)
-- UNION
-- (SELECT s.id as sid, s.OWNING_item_source, s.share_type, oc_group_user.uid as share_target
-- FROM oc_share s
-- JOIN oc_group_user ON s.ACCESSOR_share_with_group = oc_group_user.OWNING_gid
-- WHERE (s.share_type = 1) AND oc_group_user.uid = ?
--    )
-- ORDER BY sid ASC
-- "';

EXPLAIN PRIVACY;