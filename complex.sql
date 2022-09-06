SET echo;

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

INSERT INTO chat VALUES (1, 1, 2, 'HELLO from alice');

SELECT * FROM chat;

GDPR FORGET Users 1;

UPDATE chat SET message = 'hacked' WHERE id = 1;

GDPR FORGET Users 2;
SELECT * FROM chat;
/*calling gdpr get also calls resubscribe*/
GDPR GET Users 1;
SELECT * FROM chat;
SELECT * FROM chat WHERE OWNER_receiver_id = 2;
