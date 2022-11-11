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

INSERT INTO admins(id, name) VALUES (1, 'Alice');

INSERT INTO customers VALUES (10, 'Barb');
INSERT INTO customers VALUES (20, 'Chris');

INSERT INTO spi VALUES (1, 10, 1, "credit card: 1324");
INSERT INTO spi VALUES (2, 10, 1, "debit card: 3214");
INSERT INTO spi VALUES (3, 20, 1, "credit card: 4123");

INSERT INTO ppi VALUES (1, 1, 10, "zip: 53123");
INSERT INTO ppi VALUES (2, 1, 10, "spent: $231.20");
INSERT INTO ppi VALUES (3, 1, 20, "zip: 08520");

GDPR GET customers 10;
GDPR GET admins 1;
GDPR FORGET customers 10;
GDPR GET admins 1;
GDPR GET customers 10;  
GDPR GET customers 20;
