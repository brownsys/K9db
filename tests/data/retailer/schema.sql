CREATE TABLE admins ( \
  id int, \
  PII_name text, \
  PRIMARY KEY(id) \
);
CREATE TABLE customers ( \
  id int, \
  demographic_group int, \
  PII_name text, \
  PRIMARY KEY(id) \
);

CREATE VIEW q1 FOR ads AS '"SELECT * FROM customers WHERE demographic_group = 0"';
