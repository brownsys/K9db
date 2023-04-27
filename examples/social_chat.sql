SET echo;

CREATE DATA_SUBJECT TABLE users(
  ID INT,
  name TEXT,
  PRIMARY KEY(ID)
);

CREATE TABLE chat (
  ID INT,
  sender_id INT,
  receiver_id INT,
  message TEXT,
  PRIMARY KEY (ID),
  FOREIGN KEY (sender_id) OWNED_BY users(ID),
  FOREIGN KEY (receiver_id) OWNED_BY users(ID),
  -- When the data subject pointed to by sender_id issues a GDPR FORGET
  -- Anonymize the sender_id column by setting it to NULL.
  ON DEL sender_id ANON (sender_id),
  -- Ditto for receiver_id.
  ON DEL receiver_id ANON (receiver_id)
);

CREATE TABLE stories(
  ID INT,
  author INT,
  context TEXT,
  PRIMARY KEY (ID),
  FOREIGN KEY (author) OWNED_BY users(ID)
);

CREATE TABLE comments(
  ID INT,
  author INT,
  story_id INT,
  content TEXT,
  PRIMARY KEY(ID),
  FOREIGN KEY (author) OWNED_BY users(ID),
  -- no annotations: only the author has rights to the comment.
  FOREIGN KEY (story_id) REFERENCES stories(ID)
);

INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');

INSERT INTO chat VALUES (1, 1, 2, 'Msg 1');
INSERT INTO chat VALUES (2, 2, 1, 'Msg 2');
INSERT INTO chat VALUES (3, 1, 1, 'Msg 3');

INSERT INTO stories VALUES (1, 1, 'Story 1');
INSERT INTO comments VALUES (1, 2, 1, 'Comment');
INSERT INTO comments VALUES (2, 1, 1, 'Response');
