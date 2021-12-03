
-- CREATE TABLE IF NOT EXISTS "oc_appconfig" (
--   "appid" VARCHAR(32) DEFAULT '' NOT NULL,
--   "configkey" VARCHAR(64) DEFAULT '' NOT NULL,
--   "configvalue" CLOB DEFAULT NULL,
--   PRIMARY KEY("appid", "configkey")
-- );

-- CREATE INDEX appconfig_config_key_index ON "oc_appconfig" ("configkey");
-- CREATE INDEX appconfig_appid_key ON "oc_appconfig" ("appid");

CREATE TABLE oc_storages (
  id VARCHAR(64) ,
  numeric_id INT NOT NULL,
  available INT NOT NULL,
  last_checked INT,
  PRIMARY KEY (numeric_id)
);

-- CREATE UNIQUE INDEX storages_id_index ON "oc_storages" ("id");

-- CREATE TABLE IF NOT EXISTS "oc_mounts" (
--   "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   "storage_id" INTEGER NOT NULL REFERENCES oc_storages(id),
--   "root_id" BIGINT NOT NULL,
--   "user_id" VARCHAR(64) NOT NULL REFERENCES oc_users(uid),
--   "mount_point" VARCHAR(4000) NOT NULL
-- );

-- CREATE INDEX mounts_user_index ON "oc_mounts" ("user_id");
-- CREATE INDEX mounts_storage_index ON "oc_mounts" ("storage_id");
-- CREATE INDEX mounts_root_index ON "oc_mounts" ("root_id");
-- CREATE UNIQUE INDEX mounts_user_root_index ON "oc_mounts" ("user_id", "root_id");

-- CREATE TABLE IF NOT EXISTS "oc_mimetypes" (
--   "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   "mimetype" VARCHAR(255) DEFAULT '' NOT NULL
-- );

-- CREATE UNIQUE INDEX mimetype_id_index ON "oc_mimetypes" ("mimetype");

-- CREATE TABLE IF NOT EXISTS "oc_group_user" (
--   "gid" VARCHAR(64) DEFAULT '' NOT NULL REFERENCES oc_groups(gid),
--   "uid" VARCHAR(64) DEFAULT '' NOT NULL REFERENCES oc_usrs(uid),
--   PRIMARY KEY("gid", "uid")
-- );

-- CREATE INDEX gu_uid_index ON "oc_group_user" ("uid");

-- CREATE TABLE IF NOT EXISTS "oc_group_admin" (
--   "gid" VARCHAR(64) DEFAULT '' NOT NULL REFERENCES oc_groups(gid),
--   "uid" VARCHAR(64) DEFAULT '' NOT NULL REFERENCES oc_users(uid),
--   PRIMARY KEY("gid", "uid")
-- );

-- CREATE INDEX group_admin_uid ON "oc_group_admin" ("uid");

-- CREATE TABLE IF NOT EXISTS "oc_groups" (
--   "gid" VARCHAR(64) DEFAULT '' NOT NULL,
--   PRIMARY KEY("gid")
-- );

-- CREATE TABLE IF NOT EXISTS "oc_preferences" (
--   "userid" VARCHAR(64) DEFAULT '' NOT NULL REFERENCES oc_users(uid),
--   "appid" VARCHAR(32) DEFAULT '' NOT NULL,
--   "configkey" VARCHAR(64) DEFAULT '' NOT NULL,
--   "configvalue" CLOB DEFAULT NULL,
--   PRIMARY KEY("userid", "appid", "configkey")
-- );

-- CREATE TABLE IF NOT EXISTS "oc_jobs" (
--   "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   "class" VARCHAR(255) DEFAULT '' NOT NULL,
--   "argument" VARCHAR(4000) DEFAULT '' NOT NULL,
--   "last_run" INTEGER DEFAULT 0,
--   "last_checked" INTEGER DEFAULT 0,
--   "reserved_at" INTEGER DEFAULT 0,
--   execution_duration INTEGER DEFAULT -1 NOT NULL
-- );

-- CREATE INDEX job_class_index ON "oc_jobs" ("class");

-- CREATE TABLE IF NOT EXISTS "oc_vcategory" (
--   "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   "uid" VARCHAR(64) DEFAULT '' NOT NULL REFERENCES oc_users(uid),
--   "type" VARCHAR(64) DEFAULT '' NOT NULL,
--   "category" VARCHAR(255) DEFAULT '' NOT NULL
-- );

-- CREATE INDEX uid_index ON "oc_vcategory" ("uid");
-- CREATE INDEX type_index ON "oc_vcategory" ("type");
-- CREATE INDEX category_index ON "oc_vcategory" ("category");

-- CREATE TABLE IF NOT EXISTS "oc_vcategory_to_object" (
--   "objid" BIGINT UNSIGNED DEFAULT 0 NOT NULL,
--   "categoryid" INTEGER UNSIGNED DEFAULT 0 NOT NULL REFERENCES oc_category(id),
--   "type" VARCHAR(64) DEFAULT '' NOT NULL,
--   PRIMARY KEY("categoryid", "objid", "type")
-- );

-- CREATE INDEX vcategory_objectd_index ON "oc_vcategory_to_object" ("objid", "type");

-- CREATE TABLE IF NOT EXISTS "oc_systemtag" (
--   "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   "name" VARCHAR(64) DEFAULT '' NOT NULL,
--   "visibility" SMALLINT DEFAULT 1 NOT NULL,
--   "editable" SMALLINT DEFAULT 1 NOT NULL,
--   "assignable" SMALLINT DEFAULT 1 NOT NULL
-- );

-- CREATE UNIQUE INDEX tag_ident ON "oc_systemtag" ("name", "visibility", "editable", "assignable");

-- CREATE TABLE IF NOT EXISTS "oc_systemtag_object_mapping" (
--   "objectid" VARCHAR(64) DEFAULT '' NOT NULL,
--   "objecttype" VARCHAR(64) DEFAULT '' NOT NULL,
--   "systemtagid" INTEGER UNSIGNED DEFAULT 0 NOT NULL REFERENCES oc_systemtag(id)
-- );

-- CREATE UNIQUE INDEX mapping ON "oc_systemtag_object_mapping" ("objecttype", "objectid", "systemtagid");

-- CREATE TABLE IF NOT EXISTS "oc_systemtag_group" (
--   "systemtagid" INTEGER UNSIGNED DEFAULT 0 NOT NULL REFERENCES oc_systemtag(id),
--   "gid" VARCHAR(255) NOT NULL REFERENCES oc_group(gid),
--   PRIMARY KEY("gid", "systemtagid")
-- );

-- CREATE TABLE IF NOT EXISTS "oc_privatedata" (
--   "keyid" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   "user" VARCHAR(64) DEFAULT '' NOT NULL REFERENCES oc_user(uid),
--   "app" VARCHAR(255) DEFAULT '' NOT NULL,
--   "key" VARCHAR(255) DEFAULT '' NOT NULL,
--   "value" VARCHAR(255) DEFAULT '' NOT NULL
-- );

-- CREATE TABLE IF NOT EXISTS "oc_file_locks" (
--   "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   "lock" INTEGER DEFAULT 0 NOT NULL,
--   "key" VARCHAR(64) NOT NULL,
--   "ttl" INTEGER DEFAULT -1 NOT NULL
-- );

-- CREATE UNIQUE INDEX lock_key_index ON "oc_file_locks" ("key");
-- CREATE INDEX lock_ttl_index ON "oc_file_locks" ("ttl");

-- CREATE TABLE IF NOT EXISTS "oc_comments" (
--   "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   "parent_id" INTEGER UNSIGNED DEFAULT 0 NOT NULL, -- I think this *should* be a self reference, but with this default and a not-null, i'm not sure it would work correctly
--   "topmost_parent_id" INTEGER UNSIGNED DEFAULT 0 NOT NULL,
--   "children_count" INTEGER UNSIGNED DEFAULT 0 NOT NULL,
--   "actor_type" VARCHAR(64) DEFAULT '' NOT NULL,
--   "actor_id" VARCHAR(64) DEFAULT '' NOT NULL, -- This looks like it encodes a variant and could reference a user
--   "message" CLOB DEFAULT NULL,
--   "verb" VARCHAR(64) DEFAULT NULL,
--   "creation_timestamp" DATETIME DEFAULT NULL,
--   "latest_child_timestamp" DATETIME DEFAULT NULL,
--   "object_type" VARCHAR(64) DEFAULT '' NOT NULL,
--   "object_id" VARCHAR(64) DEFAULT '' NOT NULL
-- );

-- CREATE INDEX comments_parent_id_index ON "oc_comments" ("parent_id");
-- CREATE INDEX comments_topmost_parent_id_idx ON "oc_comments" ("topmost_parent_id");
-- CREATE INDEX comments_object_index ON "oc_comments" ("object_type", "object_id", "creation_timestamp");
-- CREATE INDEX comments_actor_index ON "oc_comments" ("actor_type", "actor_id");

-- CREATE TABLE IF NOT EXISTS "oc_comments_read_markers" (
--   "user_id" VARCHAR(64) DEFAULT '' NOT NULL REFERENCES oc_users(uid),
--   "marker_datetime" DATETIME DEFAULT NULL,
--   "object_type" VARCHAR(64) DEFAULT '' NOT NULL,
--   "object_id" VARCHAR(64) DEFAULT '' NOT NULL
-- );

-- CREATE INDEX comments_marker_object_index ON "oc_comments_read_markers" ("object_type", "object_id");
-- CREATE UNIQUE INDEX comments_marker_index ON "oc_comments_read_markers" ("user_id", "object_type", "object_id");

-- CREATE TABLE IF NOT EXISTS "oc_credentials" (
--   "user" VARCHAR(64) NOT NULL REFERENCES oc_users(uid),
--   "identifier" VARCHAR(64) NOT NULL,
--   "credentials" CLOB DEFAULT NULL,
--   PRIMARY KEY("user", "identifier")
-- );

-- CREATE INDEX credentials_user ON "oc_credentials" ("user");

-- CREATE TABLE IF NOT EXISTS "oc_migrations" (
--   "app" VARCHAR(177) NOT NULL,
--   "version" VARCHAR(14) NOT NULL,
--   PRIMARY KEY("app", "version")
-- );

-- CREATE TABLE oc_addressbookchanges (
--   id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   uri VARCHAR(255) DEFAULT NULL,
--   synctoken INTEGER UNSIGNED DEFAULT 1 NOT NULL,
--   addressbookid INTEGER NOT NULL REFERENCES oc_addressbooks(id),
--   operation SMALLINT NOT NULL
-- );

-- CREATE INDEX addressbookid_synctoken ON oc_addressbookchanges (addressbookid, synctoken);

-- CREATE TABLE oc_addressbooks (
--   id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   principaluri VARCHAR(255) DEFAULT NULL,
--   displayname VARCHAR(255) DEFAULT NULL,
--   uri VARCHAR(255) DEFAULT NULL,
--   description VARCHAR(255) DEFAULT NULL,
--   synctoken INTEGER UNSIGNED DEFAULT 1 NOT NULL
-- );

-- CREATE UNIQUE INDEX addressbook_index ON oc_addressbooks (principaluri, uri);

-- CREATE TABLE oc_calendarchanges (
--   id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   uri VARCHAR(255) DEFAULT NULL,
--   synctoken INTEGER UNSIGNED DEFAULT 1 NOT NULL,
--   calendarid INTEGER NOT NULL REFERENCES oc_calendars(id),
--   operation SMALLINT NOT NULL
-- );

-- CREATE INDEX calendarid_synctoken ON oc_calendarchanges (calendarid, synctoken);

-- CREATE TABLE oc_cards_properties (
--   id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   addressbookid BIGINT DEFAULT 0 NOT NULL REFERENCES oc_addressbooks(id),
--   cardid BIGINT UNSIGNED DEFAULT 0 NOT NULL REFERENCES oc_cards(id),
--   name VARCHAR(64) DEFAULT NULL,
--   value VARCHAR(255) DEFAULT NULL,
--   preferred INTEGER DEFAULT 1 NOT NULL
-- );

-- CREATE INDEX card_value_index ON oc_cards_properties (value);
-- CREATE INDEX card_name_index ON oc_cards_properties (name);
-- CREATE INDEX card_contactid_index ON oc_cards_properties (cardid);

-- CREATE TABLE oc_schedulingobjects (
--   id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   principaluri VARCHAR(255) DEFAULT NULL,
--   calendardata BLOB DEFAULT NULL,
--   uri VARCHAR(255) DEFAULT NULL,
--   lastmodified INTEGER UNSIGNED DEFAULT NULL,
--   etag VARCHAR(32) DEFAULT NULL,
--   size BIGINT UNSIGNED NOT NULL
-- );

-- CREATE TABLE oc_calendarobjects (
--   id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   calendardata BLOB DEFAULT NULL,
--   uri VARCHAR(255) DEFAULT NULL COLLATE BINARY,
--   calendarid INTEGER UNSIGNED NOT NULL REFERENCES oc_calendars(id),
--   lastmodified INTEGER UNSIGNED DEFAULT NULL,
--   etag VARCHAR(32) DEFAULT NULL COLLATE BINARY,
--   size BIGINT UNSIGNED NOT NULL,
--   uid VARCHAR(255) DEFAULT NULL COLLATE BINARY REFERENCES oc_users(uid),
--   componenttype VARCHAR(8) DEFAULT NULL COLLATE BINARY,
--   firstoccurence BIGINT UNSIGNED DEFAULT NULL,
--   lastoccurence BIGINT UNSIGNED DEFAULT NULL,
--   classification INTEGER DEFAULT 0
-- );

-- CREATE UNIQUE INDEX calobjects_index ON oc_calendarobjects (calendarid, uri);

-- CREATE TABLE oc_calendars (
--   id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   principaluri VARCHAR(255) DEFAULT NULL COLLATE BINARY,
--   displayname VARCHAR(255) DEFAULT NULL COLLATE BINARY,
--   uri VARCHAR(255) DEFAULT NULL COLLATE BINARY,
--   synctoken INTEGER UNSIGNED DEFAULT 1 NOT NULL,
--   description VARCHAR(255) DEFAULT NULL COLLATE BINARY,
--   calendarorder INTEGER UNSIGNED DEFAULT 0 NOT NULL,
--   calendarcolor VARCHAR(255) DEFAULT NULL COLLATE BINARY,
--   timezone CLOB DEFAULT NULL COLLATE BINARY,
--   transparent SMALLINT DEFAULT 0 NOT NULL,
--   components VARCHAR(20) DEFAULT NULL COLLATE BINARY
-- );

-- CREATE UNIQUE INDEX calendars_index ON oc_calendars (principaluri, uri);

-- CREATE TABLE oc_calendarsubscriptions (
--   id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   uri VARCHAR(255) DEFAULT NULL COLLATE BINARY,
--   principaluri VARCHAR(255) DEFAULT NULL COLLATE BINARY,
--   source VARCHAR(255) DEFAULT NULL COLLATE BINARY,
--   displayname VARCHAR(100) DEFAULT NULL COLLATE BINARY,
--   refreshrate VARCHAR(10) DEFAULT NULL COLLATE BINARY,
--   calendarorder INTEGER UNSIGNED DEFAULT 0 NOT NULL,
--   calendarcolor VARCHAR(255) DEFAULT NULL COLLATE BINARY,
--   striptodos SMALLINT DEFAULT NULL,
--   stripalarms SMALLINT DEFAULT NULL,
--   stripattachments SMALLINT DEFAULT NULL,
--   lastmodified INTEGER UNSIGNED NOT NULL
-- );

-- CREATE UNIQUE INDEX calsub_index ON oc_calendarsubscriptions (principaluri, uri);

-- CREATE TABLE oc_external_mounts (
--   mount_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   mount_point VARCHAR(128) NOT NULL,
--   storage_backend VARCHAR(64) NOT NULL REFERENCES oc_storages(id),
--   auth_backend VARCHAR(64) NOT NULL,
--   priority INTEGER DEFAULT 100 NOT NULL,
--   type INTEGER NOT NULL
-- );

-- CREATE TABLE oc_external_applicable (
--   applicable_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   mount_id BIGINT NOT NULL REFERENCES oc_external_mounts(mount_id),
--   type INTEGER NOT NULL,
--   value VARCHAR(64) DEFAULT NULL
-- );

-- CREATE INDEX applicable_mount ON oc_external_applicable (mount_id);
-- CREATE INDEX applicable_type_value ON oc_external_applicable (type, value);
-- CREATE UNIQUE INDEX applicable_type_value_mount ON oc_external_applicable (type, value, mount_id);

-- CREATE TABLE oc_external_config (
--   config_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   mount_id BIGINT NOT NULL REFERENCES oc_external_mounts(mount_id),
--   "key" VARCHAR(64) NOT NULL,
--   value CLOB NOT NULL
-- );

-- CREATE INDEX config_mount ON oc_external_config (mount_id);
-- CREATE UNIQUE INDEX config_mount_key ON oc_external_config (mount_id, "key");

-- CREATE TABLE oc_external_options (
--   option_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   mount_id BIGINT NOT NULL REFERENCES oc_external_mounts(mount_id),
--   "key" VARCHAR(64) NOT NULL,
--   value VARCHAR(256) NOT NULL
-- );

-- CREATE INDEX option_mount ON oc_external_options (mount_id);
-- CREATE UNIQUE INDEX option_mount_key ON oc_external_options (mount_id, "key");

-- CREATE TABLE oc_accounts (
--   id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   email VARCHAR(255) DEFAULT NULL COLLATE BINARY,
--   user_id VARCHAR(255) NOT NULL COLLATE BINARY,
--   lower_user_id VARCHAR(255) NOT NULL REFERENCES oc_users(uid) COLLATE BINARY,
--   display_name VARCHAR(255) DEFAULT NULL COLLATE BINARY,
--   quota VARCHAR(32) DEFAULT NULL COLLATE BINARY,
--   last_login INTEGER DEFAULT 0 NOT NULL,
--   backend VARCHAR(64) NOT NULL COLLATE BINARY,
--   home VARCHAR(1024) NOT NULL COLLATE BINARY,
--   state SMALLINT DEFAULT 0 NOT NULL --0: initial, 1: enabled, 2: disabled, 3: deleted
-- );

-- CREATE UNIQUE INDEX UNIQ_907AA303A76ED395 ON oc_accounts (user_id);
-- CREATE UNIQUE INDEX lower_user_id_index ON oc_accounts (lower_user_id);
-- CREATE INDEX display_name_index ON oc_accounts (display_name);
-- CREATE INDEX email_index ON oc_accounts (email);

-- CREATE TABLE oc_account_terms (
--   id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   account_id BIGINT UNSIGNED NOT NULL REFERENCES oc_accounts(id),
--   term VARCHAR(191) NOT NULL COLLATE BINARY
-- );

-- CREATE INDEX term_index ON oc_account_terms (term);
-- CREATE INDEX account_id_index ON oc_account_terms (account_id);

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

-- CREATE INDEX fs_storage_size ON oc_filecache (storage, size, fileid);
-- CREATE INDEX fs_storage_mimepart ON oc_filecache (storage, mimepart);
-- CREATE INDEX fs_storage_mimetype ON oc_filecache (storage, mimetype);
-- CREATE INDEX fs_parent_name_hash ON oc_filecache (parent, name);
-- CREATE UNIQUE INDEX fs_storage_path_hash ON oc_filecache (storage, path_hash);

-- CREATE TABLE oc_persistent_locks (
--   id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   file_id BIGINT NOT NULL REFERENCES oc_file_cache(fileid) --FK to fileid in table oc_file_cache
--   , owner_account_id BIGINT UNSIGNED DEFAULT NULL REFERENCES oc_accounts(id) --owner of the lock - FK to account table
--   , owner VARCHAR(100) DEFAULT NULL --owner of the lock - just a human readable string
--   , timeout INTEGER UNSIGNED NOT NULL --timestamp when the lock expires
--   , created_at INTEGER UNSIGNED NOT NULL --timestamp when the lock was created
--   , token VARCHAR(1024) NOT NULL --uuid for webdav locks - 1024 random chars for WOPI locks
--   , token_hash VARCHAR(32) NOT NULL --md5(token)
--   , scope SMALLINT NOT NULL --1 - exclusive, 2 - shared
--   , depth SMALLINT NOT NULL --0, 1 or infinite
-- );

-- CREATE UNIQUE INDEX UNIQ_F0C3D55BB3BC57DA ON oc_persistent_locks (token_hash);
-- CREATE INDEX IDX_F0C3D55B93CB796C ON oc_persistent_locks (file_id);
-- CREATE INDEX IDX_F0C3D55BC901C6FF ON oc_persistent_locks (owner_account_id);

-- CREATE TABLE oc_authtoken (
--   id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   uid VARCHAR(64) DEFAULT '' NOT NULL REFERENCES oc_users(uid) COLLATE BINARY,
--   password CLOB DEFAULT NULL COLLATE BINARY,
--   name CLOB DEFAULT '' NOT NULL COLLATE BINARY,
--   token VARCHAR(200) DEFAULT '' NOT NULL COLLATE BINARY,
--   type SMALLINT UNSIGNED DEFAULT 0 NOT NULL,
--   last_activity INTEGER UNSIGNED DEFAULT 0 NOT NULL,
--   last_check INTEGER UNSIGNED DEFAULT 0 NOT NULL,
--   login_name VARCHAR(255) DEFAULT '' NOT NULL COLLATE BINARY
-- );

-- CREATE INDEX authtoken_last_activity_index ON oc_authtoken (last_activity);
-- CREATE UNIQUE INDEX authtoken_token_index ON oc_authtoken (token);

-- CREATE TABLE oc_dav_shares (
--   id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   principaluri VARCHAR(255) DEFAULT NULL COLLATE BINARY,
--   type VARCHAR(255) DEFAULT NULL COLLATE BINARY,
--   access SMALLINT DEFAULT NULL,
--   resourceid INTEGER UNSIGNED NOT NULL,
--   publicuri VARCHAR(255) DEFAULT NULL
-- );

-- CREATE UNIQUE INDEX dav_shares_index ON oc_dav_shares (principaluri, resourceid, type, publicuri);

-- CREATE TABLE oc_cards (
--   id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   addressbookid INTEGER DEFAULT 0 NOT NULL REFERENCES oc_addressbook(id),
--   carddata BLOB DEFAULT NULL,
--   uri VARCHAR(255) DEFAULT NULL COLLATE BINARY,
--   lastmodified BIGINT UNSIGNED DEFAULT NULL,
--   etag VARCHAR(32) DEFAULT NULL COLLATE BINARY,
--   size BIGINT UNSIGNED NOT NULL
-- );

-- CREATE INDEX addressbookid_uri_index ON oc_cards (addressbookid, uri);

-- CREATE TABLE oc_dav_properties (
--   id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   propertypath VARCHAR(255) DEFAULT '' NOT NULL,
--   propertyname VARCHAR(255) DEFAULT '' NOT NULL,
--   propertyvalue VARCHAR(255) NOT NULL,
--   propertytype SMALLINT DEFAULT 1 NOT NULL
-- );

-- CREATE INDEX propertypath_index ON oc_dav_properties (propertypath);

-- CREATE TABLE oc_dav_job_status (
--   id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   uuid CHAR(36) NOT NULL --(DC2Type:guid)
--   , user_id VARCHAR(64) NOT NULL REFERENCES oc_users(uid),
--   status_info VARCHAR(4000) NOT NULL
-- );

-- CREATE UNIQUE INDEX UNIQ_18BBA548D17F50A6 ON oc_dav_job_status (uuid);

-- CREATE TABLE oc_properties (
--   id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   propertyname VARCHAR(255) DEFAULT '' NOT NULL COLLATE BINARY,
--   propertyvalue VARCHAR(255) NOT NULL COLLATE BINARY,
--   fileid BIGINT UNSIGNED NOT NULL REFERENCES oc_filecache(fileid),
--   propertytype SMALLINT DEFAULT 1 NOT NULL
-- );

-- CREATE INDEX fileid_index ON oc_properties (fileid);

-- CREATE TABLE IF NOT EXISTS "oc_activity_mq" (
--   "mail_id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   "amq_timestamp" INTEGER DEFAULT 0 NOT NULL,
--   "amq_latest_send" INTEGER DEFAULT 0 NOT NULL,
--   "amq_type" VARCHAR(255) NOT NULL,
--   "amq_affecteduser" VARCHAR(64) NOT NULL REFERENCES oc_users(uid),
--   "amq_appid" VARCHAR(255) NOT NULL,
--   "amq_subject" VARCHAR(255) NOT NULL,
--   "amq_subjectparams" VARCHAR(4000) NOT NULL
-- );

-- CREATE INDEX amp_user ON "oc_activity_mq" ("amq_affecteduser");
-- CREATE INDEX amp_latest_send_time ON "oc_activity_mq" ("amq_latest_send");
-- CREATE INDEX amp_timestamp_time ON "oc_activity_mq" ("amq_timestamp");

-- CREATE TABLE oc_activity (
--   activity_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   timestamp INTEGER DEFAULT 0 NOT NULL,
--   priority INTEGER DEFAULT 0 NOT NULL,
--   type VARCHAR(255) DEFAULT NULL COLLATE BINARY,
--   user VARCHAR(64) DEFAULT NULL REFERENCES oc_users(uid) COLLATE BINARY,
--   affecteduser VARCHAR(64) NOT NULL REFERENCES oc_users(uid) COLLATE BINARY,
--   app VARCHAR(255) NOT NULL COLLATE BINARY,
--   subject VARCHAR(255) NOT NULL COLLATE BINARY,
--   subjectparams CLOB NOT NULL COLLATE BINARY,
--   message VARCHAR(255) DEFAULT NULL COLLATE BINARY,
--   messageparams CLOB DEFAULT NULL COLLATE BINARY,
--   file VARCHAR(4000) DEFAULT NULL COLLATE BINARY,
--   link VARCHAR(4000) DEFAULT NULL COLLATE BINARY,
--   object_type VARCHAR(255) DEFAULT NULL COLLATE BINARY,
--   object_id BIGINT DEFAULT 0 NOT NULL
-- );

-- CREATE INDEX activity_object ON oc_activity (object_type, object_id);
-- CREATE INDEX activity_filter_app ON oc_activity (affecteduser, app, timestamp);
-- CREATE INDEX activity_filter_by ON oc_activity (affecteduser, user, timestamp);
-- CREATE INDEX activity_user_time ON oc_activity (affecteduser, timestamp);
-- CREATE INDEX activity_time ON oc_activity (timestamp);

-- CREATE TABLE oc_federated_reshares (
--   share_id BIGINT NOT NULL,
--   remote_id VARCHAR(255) NOT NULL --share ID at the remote server
-- );

-- CREATE UNIQUE INDEX share_id_index ON oc_federated_reshares (share_id);

-- CREATE TABLE IF NOT EXISTS "oc_trusted_servers" (
--   "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   "url" VARCHAR(512) NOT NULL --Url of trusted server
--   , "url_hash" VARCHAR(255) DEFAULT '' NOT NULL --sha1 hash of the url without the protocol
--   , "token" VARCHAR(128) DEFAULT NULL --token used to exchange the shared secret
--   , "shared_secret" VARCHAR(256) DEFAULT NULL --shared secret used to authenticate
--   , "status" INTEGER DEFAULT 2 NOT NULL --current status of the connection
--   , "sync_token" VARCHAR(512) DEFAULT NULL --cardDav sync token
-- );

-- CREATE UNIQUE INDEX url_hash ON "oc_trusted_servers" ("url_hash");


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
  uid_owner VARCHAR(64) NOT NULL REFERENCES oc_users(uid),
  uid_initiator VARCHAR(64) REFERENCES oc_users(uid),
  parent INT ,
  item_type VARCHAR(64) NOT NULL ,
  item_source VARCHAR(255),
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

-- CREATE INDEX item_source_index ON oc_share (item_source);
-- CREATE INDEX item_share_type_index ON oc_share (item_type, share_type);
-- CREATE INDEX file_source_index ON oc_share (file_source);
-- CREATE INDEX token_index ON oc_share (token);
-- CREATE INDEX share_with_index ON oc_share (share_with);
-- CREATE INDEX item_source_type_index ON oc_share (item_source, share_type, item_type);

-- CREATE TABLE oc_share_external (
--   id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   remote VARCHAR(512) NOT NULL COLLATE BINARY --Url of the remote owncloud instance
--   , share_token VARCHAR(64) NOT NULL COLLATE BINARY --Public share token
--   , password VARCHAR(64) DEFAULT NULL COLLATE BINARY --Optional password for the public share
--   , owner VARCHAR(64) NOT NULL COLLATE BINARY --User that owns the public share on the remote server
--   , user VARCHAR(64) NOT NULL REFERENCES oc_users(uid) COLLATE BINARY --Local user which added the external share
--   , mountpoint VARCHAR(4000) NOT NULL COLLATE BINARY --Full path where the share is mounted
--   , mountpoint_hash VARCHAR(32) NOT NULL COLLATE BINARY --md5 hash of the mountpoint
--   , accepted INTEGER DEFAULT 0 NOT NULL,
--   remote_id VARCHAR(255) DEFAULT '-1' NOT NULL COLLATE BINARY,
--   lastscan BIGINT UNSIGNED DEFAULT NULL,
--   name VARCHAR(255) NOT NULL COLLATE BINARY --Original name on the remote server
-- );
-- CREATE INDEX sh_external_user ON oc_share_external (user);
-- CREATE UNIQUE INDEX sh_external_mp ON oc_share_external (user, mountpoint_hash);

-- CREATE TABLE oc_files_trash (
--   auto_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   id VARCHAR(250) DEFAULT '' NOT NULL,
--   user VARCHAR(64) DEFAULT '' NOT NULL REFERENCES oc_users(uid),
--   timestamp VARCHAR(12) DEFAULT '' NOT NULL,
--   location VARCHAR(512) DEFAULT '' NOT NULL,
--   type VARCHAR(4) DEFAULT NULL,
--   mime VARCHAR(255) DEFAULT NULL
-- );

-- CREATE INDEX id_index ON oc_files_trash (id);
-- CREATE INDEX timestamp_index ON oc_files_trash (timestamp);
-- CREATE INDEX user_index ON oc_files_trash (user);

-- CREATE TABLE oc_notifications (
--   notification_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
--   app VARCHAR(32) NOT NULL COLLATE BINARY,
--   user VARCHAR(64) NOT NULL REFERENCES oc_users(uid) COLLATE BINARY,
--   timestamp INTEGER DEFAULT 0 NOT NULL,
--   object_type VARCHAR(64) NOT NULL COLLATE BINARY,
--   object_id VARCHAR(64) NOT NULL COLLATE BINARY,
--   subject VARCHAR(64) NOT NULL COLLATE BINARY,
--   subject_parameters CLOB DEFAULT NULL COLLATE BINARY,
--   message_parameters CLOB DEFAULT NULL COLLATE BINARY,
--   actions CLOB DEFAULT NULL COLLATE BINARY,
--   icon VARCHAR(4000) DEFAULT NULL COLLATE BINARY,
--   message VARCHAR(64) DEFAULT NULL COLLATE BINARY,
--   link VARCHAR(4000) DEFAULT NULL COLLATE BINARY
-- );

-- CREATE INDEX IDX_16B8074811CB6B3A232D562B ON oc_notifications (object_type, object_id);
-- CREATE INDEX IDX_16B80748A5D6E63E ON oc_notifications (timestamp);
-- CREATE INDEX IDX_16B807488D93D649 ON oc_notifications (user);
-- CREATE INDEX IDX_16B80748C96E70CF ON oc_notifications (app);
