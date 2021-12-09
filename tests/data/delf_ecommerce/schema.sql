CREATE TABLE admins ( \
  id int, \
  PII_name text, \
  PRIMARY KEY(id) \
);

CREATE TABLE customers ( \
  id int, \
  PII_name text, \
  PRIMARY KEY(id) \
);

CREATE TABLE spi /* sensitive payment info */ ( \
  id int, \
  OWNER_customer_id int, \
  ACCESSOR_admin_id int, \
  message text, \
  PRIMARY KEY(id), \
  FOREIGN KEY (OWNER_customer_id) REFERENCES customers(id), \
  FOREIGN KEY (ACCESSOR_admin_id) REFERENCES admins(id) \
);

CREATE TABLE ppi /* preserved payment info */ ( \
  id int, \
  OWNER_admin_id int, \
  ACCESSOR_customer_id int, \
  message text, \
  PRIMARY KEY(id), \
  FOREIGN KEY (OWNER_admin_id) REFERENCES admins(id), \
  FOREIGN KEY (ACCESSOR_customer_id) REFERENCES customers(id) \
);