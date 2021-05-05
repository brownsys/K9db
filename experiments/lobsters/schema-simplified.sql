SET echo;
CREATE TABLE users ( \
  id int NOT NULL PRIMARY KEY, \
  PII_username varchar(50), \
  email varchar(100), \
  password_digest varchar(75), \
  created_at datetime, \
  is_admin int, \
  password_reset_token varchar(75), \
  session_token varchar(75) NOT NULL, \
  about text, \
  invited_by_user_id int, \
  is_moderator int, \
  pushover_mentions int, \
  rss_token varchar(75), \
  mailing_list_token varchar(75), \
  mailing_list_mode int, \
  karma int NOT NULL, \
  banned_at datetime, \
  banned_by_user_id int, \
  banned_reason varchar(200), \
  deleted_at datetime, \
  disabled_invite_at datetime, \
  disabled_invite_by_user_id int, \
  disabled_invite_reason varchar(200), \
  settings text, \
  FOREIGN KEY (banned_by_user_id) REFERENCES users(id), \
  FOREIGN KEY (invited_by_user_id) REFERENCES users(id), \
  FOREIGN KEY (disabled_invite_by_user_id) REFERENCES users(id) \
) ENGINE=ROCKSDB DEFAULT CHARSET=utf8;
CREATE TABLE comments ( \
  id int NOT NULL PRIMARY KEY, \
  created_at datetime NOT NULL, \
  updated_at datetime, \
  short_id varchar(10) NOT NULL, \
  story_id int NOT NULL, \
  user_id int NOT NULL, \
  parent_comment_id int, \
  thread_id int, \
  comment text NOT NULL, \
  upvotes int NOT NULL, \
  downvotes int NOT NULL, \
  confidence int NOT NULL, \
  markeddown_comment text, \
  is_deleted int, \
  is_moderated int, \
  is_from_email int, \
  hat_id int, \
  FOREIGN KEY (user_id) REFERENCES users(id) \
) ENGINE=ROCKSDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE hat_requests ( \
  id int NOT NULL PRIMARY KEY, \
  created_at datetime, \
  updated_at datetime, \
  user_id int, \
  hat varchar(255), \
  link varchar(255), \
  comment text, \
  FOREIGN KEY (user_id) REFERENCES users(id) \
) ENGINE=ROCKSDB DEFAULT CHARSET=utf8;
CREATE TABLE hats ( \
  id int NOT NULL PRIMARY KEY, \
  created_at datetime, \
  updated_at datetime, \
  OWNER_user_id int, \
  OWNER_granted_by_user_id int, \
  hat varchar(255) NOT NULL, \
  link varchar(255), \
  modlog_use int, \
  doffed_at datetime, \
  FOREIGN KEY (OWNER_user_id) REFERENCES users(id), \
  FOREIGN KEY (OWNER_granted_by_user_id) REFERENCES users(id) \
) ENGINE=ROCKSDB DEFAULT CHARSET=utf8;
CREATE TABLE hidden_stories ( \
  id int NOT NULL PRIMARY KEY, \
  user_id int, \
  story_id int, \
  FOREIGN KEY (user_id) REFERENCES users(id) \
) ENGINE=ROCKSDB DEFAULT CHARSET=utf8;
CREATE TABLE invitation_requests ( \
  id int NOT NULL PRIMARY KEY, \
  code varchar(255), \
  is_verified int, \
  PII_email varchar(255), \
  name varchar(255), \
  memo text, \
  ip_address varchar(255), \
  created_at datetime NOT NULL, \
  updated_at datetime NOT NULL \
) ENGINE=ROCKSDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE invitations ( \
  id int NOT NULL PRIMARY KEY, \
  OWNER_user_id int, \
  OWNER_email varchar(255), \
  code varchar(255), \
  created_at datetime NOT NULL, \
  updated_at datetime NOT NULL, \
  memo text, \
  FOREIGN KEY (OWNER_user_id) REFERENCES users(id) \
) ENGINE=ROCKSDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE keystores ( \
  keyX varchar(50) NOT NULL PRIMARY KEY, \
  valueX int \
) ENGINE=ROCKSDB DEFAULT CHARSET=utf8;
CREATE TABLE messages ( \
  id int NOT NULL PRIMARY KEY, \
  created_at datetime, \
  OWNER_author_user_id int, \
  OWNER_recipient_user_id int, \
  has_been_read int, \
  subject varchar(100), \
  body text, \
  short_id varchar(30), \
  deleted_by_author int, \
  deleted_by_recipient int, \
  FOREIGN KEY (OWNER_author_user_id) REFERENCES users(id), \
  FOREIGN KEY (OWNER_recipient_user_id) REFERENCES users(id) \
) ENGINE=ROCKSDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE moderations ( \
  id int NOT NULL PRIMARY KEY, \
  created_at datetime NOT NULL, \
  updated_at datetime NOT NULL, \
  OWNER_moderator_user_id int, \
  story_id int, \
  comment_id int, \
  OWNER_user_id int, \
  `action` text, \
  reason text, \
  is_from_suggestions int, \
  FOREIGN KEY (OWNER_user_id) REFERENCES users(id), \
  FOREIGN KEY (OWNER_moderator_user_id) REFERENCES users(id) \
) ENGINE=ROCKSDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE read_ribbons ( \
  id int NOT NULL PRIMARY KEY, \
  is_following int, \
  created_at datetime NOT NULL, \
  updated_at datetime NOT NULL, \
  user_id int, \
  story_id int, \
  FOREIGN KEY (user_id) REFERENCES users(id) \
) ENGINE=ROCKSDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE saved_stories ( \
  id int NOT NULL PRIMARY KEY, \
  created_at datetime NOT NULL, \
  updated_at datetime NOT NULL, \
  user_id int, \
  story_id int, \
  FOREIGN KEY (user_id) REFERENCES users(id) \
) ENGINE=ROCKSDB DEFAULT CHARSET=utf8;
CREATE TABLE stories ( \
  id int NOT NULL PRIMARY KEY, \
  created_at datetime, \
  user_id int, \
  url varchar(250), \
  title varchar(150) NOT NULL, \
  description text, \
  short_id varchar(6) NOT NULL, \
  is_expired int NOT NULL, \
  upvotes int NOT NULL, \
  downvotes int NOT NULL, \
  is_moderated int NOT NULL, \
  hotness int NOT NULL, \
  markeddown_description text, \
  story_cache text, \
  comments_count int NOT NULL, \
  merged_story_id int, \
  unavailable_at datetime, \
  twitter_id varchar(20), \
  user_is_author int, \
  FOREIGN KEY (user_id) REFERENCES users(id) \
) ENGINE=ROCKSDB DEFAULT CHARSET=utf8mb4;
-- Need this index for transitive sharding.
CREATE INDEX storiespk ON stories(id);
CREATE TABLE tags ( \
  id int NOT NULL PRIMARY KEY, \
  tag varchar(25) NOT NULL, \
  description varchar(100), \
  privileged int, \
  is_media int, \
  inactive int, \
  hotness_mod int \
) ENGINE=ROCKSDB DEFAULT CHARSET=utf8;
CREATE TABLE suggested_taggings ( \
  id int NOT NULL PRIMARY KEY, \
  story_id int, \
  tag_id int, \
  user_id int, \
  FOREIGN KEY (user_id) REFERENCES users(id) \
) ENGINE=ROCKSDB DEFAULT CHARSET=utf8;
CREATE TABLE suggested_titles ( \
  id int NOT NULL PRIMARY KEY, \
  story_id int, \
  user_id int, \
  title varchar(150) NOT NULL, \
  FOREIGN KEY (user_id) REFERENCES users(id) \
) ENGINE=ROCKSDB DEFAULT CHARSET=utf8;
CREATE TABLE tag_filters ( \
  id int NOT NULL PRIMARY KEY, \
  created_at datetime NOT NULL, \
  updated_at datetime NOT NULL, \
  user_id int, \
  tag_id int, \
  FOREIGN KEY (user_id) REFERENCES users(id) \
) ENGINE=ROCKSDB DEFAULT CHARSET=utf8;
CREATE TABLE taggings ( \
  id int NOT NULL PRIMARY KEY, \
  story_id int NOT NULL, \
  tag_id int NOT NULL, \
  FOREIGN KEY (tag_id) REFERENCES tags(id), \
  FOREIGN KEY (story_id) REFERENCES stories(id) \
) ENGINE=ROCKSDB DEFAULT CHARSET=utf8;
CREATE TABLE votes ( \
  id int NOT NULL PRIMARY KEY, \
  OWNER_user_id int NOT NULL, \
  story_id int NOT NULL, \
  comment_id int, \
  vote int NOT NULL, \
  reason varchar(1), \
  FOREIGN KEY (OWNER_user_id) REFERENCES users(id), \
  FOREIGN KEY (story_id) REFERENCES stories(id), \
  FOREIGN KEY (comment_id) REFERENCES comments(id) \
) ENGINE=ROCKSDB DEFAULT CHARSET=utf8;
