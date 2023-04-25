SET echo;

CREATE DATA_SUBJECT TABLE Users (
  id int,
  name text,
  PRIMARY KEY(id)
);

CREATE TABLE chat (
  id int,
  sender_id int,
  receiver_id int,
  message text,
  PRIMARY KEY(id),
  FOREIGN KEY (sender_id) OWNED_BY Users(id),
  FOREIGN KEY (receiver_id) OWNED_BY Users(id),
  ON DEL sender_id ANON (sender_id),
  ON DEL receiver_id ANON (receiver_id)
);

INSERT INTO Users VALUES (1, 'Alice');
INSERT INTO Users VALUES (2, 'Bob');
INSERT INTO Users VALUES (3, 'Carl');

INSERT INTO chat VALUES (1, 1, 2, 'Msg 1');
INSERT INTO chat VALUES (2, 1, 3, 'Msg 2');
INSERT INTO chat VALUES (3, 2, 1, 'Msg 3');
INSERT INTO chat VALUES (4, 1, 1, 'Msg 4');
INSERT INTO chat VALUES (5, 2, 3, 'Msg 5');
