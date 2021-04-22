CREATE TABLE users ( \
  id int NOT NULL PRIMARY KEY, \
  PII_username varchar(50), \
  karma int NOT NULL \
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE comments ( \
  id int NOT NULL PRIMARY KEY, \
  created_at datetime NOT NULL, \
  updated_at datetime, \
  short_id varchar(10) NOT NULL, \
  story_id int NOT NULL, \
  OWNER_user_id int NOT NULL, \
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
  FOREIGN KEY (OWNER_user_id) REFERENCES users(id) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE hat_requests ( \
  id int NOT NULL PRIMARY KEY, \
  created_at datetime, \
  updated_at datetime, \
  OWNER_user_id int, \
  hat varchar(255), \
  link varchar(255), \
  comment text, \
  FOREIGN KEY (OWNER_user_id) REFERENCES users(id) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE hidden_stories ( \
	id int NOT NULL PRIMARY KEY, \
	OWNER_user_id int, \
	story_id int, \
  FOREIGN KEY (OWNER_user_id) REFERENCES users(id) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE invitation_requests ( \
	id int NOT NULL PRIMARY KEY, \
	code varchar(255), \
	is_verified int, \
	email varchar(255), \
	name varchar(255), \
	memo text, \
	ip_address varchar(255), \
	created_at datetime NOT NULL, \
	updated_at datetime NOT NULL \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE invitations ( \
	id int NOT NULL PRIMARY KEY, \
	OWNER_user_id int, \
	email varchar(255), \
	code varchar(255), \
	created_at datetime NOT NULL, \
	updated_at datetime NOT NULL, \
  memo text, \
  FOREIGN KEY (OWNER_user_id) REFERENCES users(id) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE keystores ( \
	`key` varchar(50) NOT NULL PRIMARY KEY, \
	value int \
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
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
  FOREIGN KEY (OWNER_moderator_user_id) REFERENCES users(id) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE read_ribbons ( \
	id int NOT NULL PRIMARY KEY, \
	is_following int, \
	created_at datetime NOT NULL, \
	updated_at datetime NOT NULL, \
	OWNER_user_id int, \
	story_id int, \
  FOREIGN KEY (OWNER_user_id) REFERENCES users(id) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE saved_stories ( \
	id int NOT NULL PRIMARY KEY, \
	created_at datetime NOT NULL, \
	updated_at datetime NOT NULL, \
	OWNER_user_id int, \
	story_id int, \
  FOREIGN KEY (OWNER_user_id) REFERENCES users(id) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE stories ( \
	id int NOT NULL PRIMARY KEY, \
	created_at datetime, \
	OWNER_user_id int, \
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
  FOREIGN KEY (OWNER_user_id) REFERENCES users(id) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE suggested_taggings ( \
	id int NOT NULL PRIMARY KEY, \
	story_id int, \
	tag_id int, \
	OWNER_user_id int, \
  FOREIGN KEY (OWNER_user_id) REFERENCES users(id) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE suggested_titles ( \
	id int NOT NULL PRIMARY KEY, \
	story_id int, \
	OWNER_user_id int, \
	title varchar(150) NOT NULL, \
  FOREIGN KEY (OWNER_user_id) REFERENCES users(id) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE tag_filters ( \
	id int NOT NULL PRIMARY KEY, \
	created_at datetime NOT NULL, \
	updated_at datetime NOT NULL, \
	OWNER_user_id int, \
	tag_id int, \
  FOREIGN KEY (OWNER_user_id) REFERENCES users(id) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE taggings ( \
	id int NOT NULL PRIMARY KEY, \
	story_id int NOT NULL, \
	tag_id int NOT NULL \
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE tags ( \
	id int NOT NULL PRIMARY KEY, \
	tag varchar(25) NOT NULL, \
	description varchar(100), \
	privileged int, \
	is_media int, \
	inactive int, \
	hotness_mod int \
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE votes ( \
	id int NOT NULL PRIMARY KEY, \
	OWNER_user_id int NOT NULL, \
	story_id int NOT NULL, \
	comment_id int, \
	vote int NOT NULL, \
	reason varchar(1), \
  FOREIGN KEY (OWNER_user_id) REFERENCES users(id) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO users VALUES (1, 'admin', 1);
INSERT INTO users VALUES (2, 'joe', 0);
INSERT INTO comments VALUES (1, '2021-04-21 01:00:00', '2021-04-21 01:00:00', 'asdf', 1, 2, 0, 0, 'yo', 0, 0, 0, 0, 0, 0, 0, 0);
INSERT INTO comments VALUES (2, '2021-04-21 01:00:00', '2021-04-21 01:00:00', 'asdf2', 2, 1, 0, 0, 'yo2', 0, 0, 0, 0, 0, 0, 0, 0);
