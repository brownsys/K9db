CREATE TABLE users1 ( \
  id int, \
  PII_name text, \
  PRIMARY KEY(id) \
);

CREATE TABLE chat1 ( \
  id int, \
  OWNER_sender_id int, \
  OWNER_receiver_id int, \
  message text, \
  PRIMARY KEY(id), \
  FOREIGN KEY (OWNER_sender_id) REFERENCES users1(id), \
  FOREIGN KEY (OWNER_receiver_id) REFERENCES users1(id) \
);
