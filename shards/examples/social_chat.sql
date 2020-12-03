CREATE TABLE Users ( \
  id int, \
  PII_name varchar(100), \
  PRIMARY KEY(id) \
);

CREATE TABLE chat ( \
  id int, \
  OWNER_sender_id int, \
  OWNER_receiver_id int, \
  message string, \
  PRIMARY KEY(id), \
  FOREIGN KEY (OWNER_sender_id) REFERENCES Users(id), \
  FOREIGN KEY (OWNER_receiver_id) REFERENCES Users(id) \
);

INSERT INTO Users VALUES (1, 'Alice');
INSERT INTO Users VALUES (2, 'Bob');
INSERT INTO Users VALUES (3, 'Carl');

INSERT INTO chat VALUES (1, 1, 2, 'HELLO from alice');
INSERT INTO chat VALUES (2, 2, 3, 'HELLO from bob');
INSERT INTO chat VALUES (3, 3, 1, 'HELLO from carl');
INSERT INTO chat VALUES (4, 3, 2, 'HELLO from carl 2');

SELECT * FROM chat;
