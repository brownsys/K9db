CREATE TABLE Users ( \
  id int, \
  PII_name text, \
  PRIMARY KEY(id) \
);

CREATE TABLE chat ( \
  id int, \
  OWNER_sender_id int, \
  OWNER_receiver_id int, \
  message text, \
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
GDPR GET Users 1;

UPDATE chat SET message = 'Hello 2' WHERE message = 'HELLO from carl 2';
UPDATE chat SET message = 'From Alice' WHERE OWNER_sender_id = 1;
UPDATE chat SET message = 'To Alice' WHERE OWNER_receiver_id = 1;
SELECT * FROM chat;

INSERT INTO chat VALUES (5, 1, 2, 'Message will move');
UPDATE chat SET OWNER_sender_id = 3 WHERE id = 5 AND OWNER_receiver_id = 2 AND OWNER_sender_id = 1;
SELECT * FROM chat WHERE id = 5;
UPDATE chat SET OWNER_sender_id = 2, OWNER_receiver_id = 3 WHERE id = 5;
SELECT * FROM chat WHERE id = 5;
SELECT * FROM chat WHERE OWNER_sender_id = 1;
SELECT * FROM chat WHERE OWNER_sender_id = 2;
SELECT * FROM chat WHERE OWNER_sender_id = 3;

DELETE FROM Users WHERE id = 1;
SELECT * FROM chat;
SELECT * FROM chat WHERE OWNER_sender_id = 1;

DELETE FROM Users WHERE id = 2;
SELECT * FROM chat;
