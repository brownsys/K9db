CREATE DATA_SUBJECT TABLE auth_user ( \
  id int, \
  ptr int, \
  username text, \
  password text, \
  PRIMARY KEY(id) \
);

CREATE TABLE shuup_contact ( \
  id int, \
  ptr int, \
  name text, \
  email text, \
  PRIMARY KEY(id) \
);

CREATE INDEX shuup_contact_ptr ON shuup_contact(ptr);

CREATE TABLE shuup_personcontact ( \
  pid int, \
  contact_ptr_id int NOT NULL, \
  name text, \
  user_id int, \
  PRIMARY KEY(pid), \
  FOREIGN KEY (contact_ptr_id) REFERENCES shuup_contact(ptr), \
  FOREIGN KEY (user_id) REFERENCES auth_user(id) \
);

CREATE TABLE shuup_companycontact ( \
  contact_ptr_id int NOT NULL, \
  tax_number int, \
  PRIMARY KEY(contact_ptr_id) \
);

CREATE TABLE shuup_companycontact_members ( \
  id int, \
  companycontact_id int NOT NULL, \
  contact_id int, \
  PRIMARY KEY (id), \
  FOREIGN KEY (companycontact_id) REFERENCES shuup_companycontact(contact_ptr_id), \
  FOREIGN KEY (contact_id) ACCESSED_BY shuup_personcontact(pid) \
);

CREATE TABLE shuup_shop ( \
  id int, \
  owner_id int NOT NULL, \
  PRIMARY KEY (id), \
  FOREIGN KEY (owner_id) OWNED_BY auth_user(id) \
);

CREATE TABLE shuup_gdpr_gdpruserconsent ( \
  id int, \
  created_on datetime, \
  shop_id int NOT NULL, \
  user_id int NOT NULL, \
  FOREIGN KEY (shop_id) REFERENCES shuup_shop(id), \
  FOREIGN KEY (user_id) OWNED_BY auth_user(id), \
  PRIMARY KEY (id) \
);

EXPLAIN COMPLIANCE;
