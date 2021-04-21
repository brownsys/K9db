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


INSERT INTO users VALUES (1, 'admin', 1);
INSERT INTO users VALUES (2, 'joe', 0);
INSERT INTO comments VALUES (1, '2021-04-21 01:00:00', '2021-04-21 01:00:00', 'asdf', 1, 2, NULL, NULL, 'yo', 0, 0, 0, NULL, 0, 0, 0, NULL);
