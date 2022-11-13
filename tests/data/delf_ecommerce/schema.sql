CREATE DATA_SUBJECT TABLE admins ( \
  id int, \
  name text, \
  PRIMARY KEY(id) \
);

CREATE DATA_SUBJECT TABLE customers ( \
  id int, \
  name text, \
  PRIMARY KEY(id) \
);

CREATE TABLE spi /* sensitive payment info */ ( \
  id int, \
  customer_id int, \
  admin_id int, \
  message text, \
  PRIMARY KEY(id), \
  FOREIGN KEY (customer_id) OWNED_BY customers(id), \
  FOREIGN KEY (admin_id) ACCESSED_BY admins(id) \
);

CREATE TABLE ppi /* preserved payment info */ ( \
  id int, \
  admin_id int, \
  customer_id int, \
  message text, \
  PRIMARY KEY(id), \
  FOREIGN KEY (admin_id) OWNED_BY admins(id), \
  FOREIGN KEY (customer_id) ACCESSED_BY customers(id) \
);
