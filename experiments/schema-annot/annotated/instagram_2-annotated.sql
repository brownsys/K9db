CREATE DATA_SUBJECT TABLE users (
  id int NOT NULL,
  username text NOT NULL,
  firstname text NOT NULL,
  surname text NOT NULL,
  email text NOT NULL,
  password text NOT NULL,
  bio text NOT NULL,
  joined text NOT NULL,
  email_verified text NOT NULL,
  account_type text NOT NULL,
  instagram text NOT NULL,
  twitter text NOT NULL,
  facebook text NOT NULL,
  github text NOT NULL,
  website text NOT NULL,
  phone text NOT NULL,
  isOnline text NOT NULL,
  lastOnline text NOT NULL,
  PRIMARY KEY (id)
);

-- Reserved keyword: k9db crashes when this table is called 'groups', so renamed to groups1
CREATE TABLE groups1 (
  id int NOT NULL,
  name text NOT NULL,
  bio text NOT NULL,
  admin int NOT NULL,
  group_type text NOT NULL,
  created text NOT NULL,
  PRIMARY KEY (id),
  FOREIGN KEY (admin) REFERENCES users(id)
);

CREATE TABLE posts (
  id int NOT NULL,
  -- Reserved keyword: renamed from 'user' to 'user_id' to avoid k9db crashing 
  user_id int NOT NULL,
  description text NOT NULL,
  imgSrc text NOT NULL,
  -- Reserved keyword: k9db crashes when column is called 'filter'
  -- filter text NOT NULL,
  location text NOT NULL,
  type text NOT NULL,
  group_id int NOT NULL,
  post_time text NOT NULL,
  PRIMARY KEY (id),
  FOREIGN KEY (user_id) OWNED_BY users(id),
  FOREIGN KEY (group_id) REFERENCES groups1(id)
);

CREATE TABLE blocks (
  block_id int NOT NULL,
  block_by int NOT NULL,
  user_id int NOT NULL,
  block_time text NOT NULL,
  PRIMARY KEY (block_id),
  FOREIGN KEY (user_id) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE bookmarks (
  bkmrk_id int NOT NULL,
  bkmrk_by int NOT NULL,
  post_id int NOT NULL,
  bkmrk_time text NOT NULL,
  PRIMARY KEY (bkmrk_id),
  FOREIGN KEY (post_id) REFERENCES posts(id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE comments (
  comment_id int NOT NULL,
  type text NOT NULL,
  text text NOT NULL,
  commentSrc text NOT NULL,
  comment_by int NOT NULL,
  post_id int NOT NULL,
  comment_time text NOT NULL,
  PRIMARY KEY (comment_id),
  FOREIGN KEY (post_id) REFERENCES posts(id),
  FOREIGN KEY (comment_by) OWNED_BY users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE conversations (
  con_id int NOT NULL,
  user_one int NOT NULL,
  user_two int NOT NULL,
  con_time text NOT NULL,
  PRIMARY KEY (con_id),
  FOREIGN KEY (user_one) OWNED_BY users(id),
  FOREIGN KEY (user_two) OWNED_BY users(id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE favourites (
  fav_id int NOT NULL,
  fav_by int NOT NULL,
  user int NOT NULL,
  fav_time text NOT NULL,
  PRIMARY KEY (fav_id),
  FOREIGN KEY (user) REFERENCES users(id),
  FOREIGN KEY (fav_by) OWNED_BY users(id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE follow_system (
  follow_id int NOT NULL,
  follow_by int NOT NULL,
  follow_by_username text NOT NULL,
  follow_to int NOT NULL,
  follow_to_username text NOT NULL,
  follow_time text NOT NULL,
  PRIMARY KEY (follow_id),
  FOREIGN KEY (follow_by) OWNED_BY users(id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE group_members (
  grp_member_id int NOT NULL,
  group_id int NOT NULL,
  member int NOT NULL,
  added_by int NOT NULL,
  joined_group text NOT NULL,
  PRIMARY KEY (grp_member_id),
  FOREIGN KEY (member) OWNED_BY users(id),
  FOREIGN KEY (added_by) ACCESSED_BY users(id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE hashtags (
  hashtag_id int NOT NULL,
  hashtag text NOT NULL,
  post_id int NOT NULL,
  -- Reserved keyword: renamed from 'user' to 'user_id' to avoid k9db crashing
  user_id int NOT NULL,
  hashtag_time text NOT NULL,
  PRIMARY KEY (hashtag_id),
  FOREIGN KEY (user_id) OWNED_BY users(id),
  FOREIGN KEY (post_id) REFERENCES posts(id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE likes (
  like_id int NOT NULL,
  post_id int NOT NULL,
  like_by int NOT NULL,
  like_time text NOT NULL,
  PRIMARY KEY (like_id),
  FOREIGN KEY (like_by) OWNED_BY users(id),
  FOREIGN KEY (post_id) REFERENCES posts(id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


CREATE TABLE messages (
  message_id int NOT NULL,
  con_id int NOT NULL,
  mssg_by int NOT NULL,
  mssg_to int NOT NULL,
  message text NOT NULL,
  type text NOT NULL,
  status text NOT NULL,
  message_time text NOT NULL,
  PRIMARY KEY (message_id),
  FOREIGN KEY (mssg_by) OWNED_BY users(id),
  FOREIGN KEY (mssg_to) OWNED_BY users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;


CREATE TABLE notifications (
  notify_id int NOT NULL,
  notify_by int NOT NULL,
  notify_to int NOT NULL,
  post_id int NOT NULL,
  group_id int NOT NULL,
  type text NOT NULL,
  user int NOT NULL,
  notify_time text NOT NULL,
  status text NOT NULL,
  PRIMARY KEY (notify_id),
  FOREIGN KEY (post_id) REFERENCES posts(id),
  FOREIGN KEY (group_id) REFERENCES groups1(id),
  FOREIGN KEY (notify_by) REFERENCES users(id),
  FOREIGN KEY (notify_to) OWNED_BY users(id),
  FOREIGN KEY (user) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE post_tags (
  post_tag_id int NOT NULL,
  post_id int NOT NULL,
  user int NOT NULL,
  PRIMARY KEY (post_tag_id),
  FOREIGN KEY (user) OWNED_BY users(id),
  FOREIGN KEY (post_id) REFERENCES posts(id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE profile_views (
  view_id int NOT NULL,
  view_by int NOT NULL,
  view_to int NOT NULL,
  view_time text NOT NULL,
  PRIMARY KEY (view_id),
  FOREIGN KEY (view_by) OWNED_BY users(id),
  FOREIGN KEY (view_to) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE recommendations (
  recommend_id int NOT NULL,
  recommend_by int NOT NULL,
  recommend_to int NOT NULL,
  recommend_of int NOT NULL,
  recommend_time text NOT NULL,
  PRIMARY KEY (recommend_id),
  FOREIGN KEY (recommend_by) OWNED_BY users(id),
  FOREIGN KEY (recommend_to) OWNED_BY users(id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE shares (
  share_id int NOT NULL,
  share_by int NOT NULL,
  share_to int NOT NULL,
  post_id int NOT NULL,
  share_time text NOT NULL,
  PRIMARY KEY (share_id),
  FOREIGN KEY (share_by) OWNED_BY users(id),
  FOREIGN KEY (share_to) ACCESSED_BY users(id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE tags (
  tag_id int NOT NULL,
  user int NOT NULL,
  tag text NOT NULL,
  PRIMARY KEY (tag_id),
  FOREIGN KEY (user) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

EXPLAIN COMPLIANCE;