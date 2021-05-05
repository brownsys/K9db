CREATE TABLE users ( 
id int NOT NULL PRIMARY KEY, 
  PII_username varchar(50), 
  karma int NOT NULL 
);
CREATE TABLE comments (
  id int NOT NULL PRIMARY KEY, 
  created_at datetime NOT NULL, 
  updated_at datetime, 
  short_id varchar(10) NOT NULL, 
  story_id int NOT NULL, 
  user_id int NOT NULL, 
  parent_comment_id int, 
  thread_id int, 
  comment text NOT NULL, 
  upvotes int NOT NULL, 
  downvotes int NOT NULL, 
  confidence int NOT NULL, 
  markeddown_comment text, 
  is_deleted int, 
  is_moderated int, 
  is_from_email int, 
  hat_id int, 
  FOREIGN KEY (user_id) REFERENCES users(id) 
);