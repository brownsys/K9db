CREATE DATA_SUBJECT TABLE users ( \
  id int, \
  name text, \
  PRIMARY KEY(id) \
);
CREATE TABLE docs ( \
  id int, \
  owner_id int, \
  body text, \
  PRIMARY KEY(id), \
  FOREIGN KEY (owner_id) OWNED_BY users(id) \
);
