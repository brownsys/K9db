CREATE DATA_SUBJECT TABLE user_info (
  id int NOT NULL,
  github_id int,
  name text NOT NULL,
  password text,
  avatar text,
  location text,
  socketid text,
  website text,
  github text,
  intro text,
  company text,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8; 

CREATE TABLE group_info (
  to_group_id text NOT NULL,
  name text NOT NULL,
  group_notice text NOT NULL,
  creator_id int NOT NULL,
  create_time int NOT NULL,
  PRIMARY KEY (to_group_id),
  FOREIGN KEY (creator_id) OWNED_BY user_info(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8; 

CREATE TABLE group_msg (
  id text NOT NULL,
  from_user text NOT NULL,
  to_group_id text NOT NULL,
  message text NOT NULL,
  time text NOT NULL,
  attachments text,
  PRIMARY KEY (id),
  -- KEY to_group (to_group_id),
  FOREIGN KEY (from_user) OWNED_BY user_info(id)
  FOREIGN KEY (to_group_id) ACCESSED_BY group_info(to_group_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE group_user_relation (
  id text NOT NULL,
  to_group_id text NOT NULL,
  user_id text NOT NULL,
  PRIMARY KEY (id),
  FOREIGN KEY (user_id) OWNED_BY user_info(id)
  FOREIGN KEY (to_group_id) ACCESSES group_info(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE private_msg (
  id text NOT NULL,
  from_user text NOT NULL,
  to_user text NOT NULL,
  message text,
  time text NOT NULL,
  attachments text,
  PRIMARY KEY (id),
  -- KEY from_user (from_user),
  -- KEY to_user (to_user),
  FOREIGN KEY (from_user) OWNED_BY user_info(id),
  FOREIGN KEY (to_user) OWNED_BY user_info(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE user_user_relation (
  id text NOT NULL,
  user_id text NOT NULL,
  from_user text NOT NULL,
  remark text,
  shield int NOT NULL,
  time text NOT NULL,
  PRIMARY KEY (id),
  FOREIGN KEY (user_id) OWNED_BY user_info(id),
  FOREIGN KEY (from_user) OWNED_BY user_info(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

EXPLAIN COMPLIANCE;