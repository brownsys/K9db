CREATE TABLE auth_user ( \
  id int, \
  ptr int, \
  PII_username text, \
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
  FOREIGN KEY (user_id) REFERENCES auth_user(ptr) \
);

CREATE INDEX auth_user_ptr ON auth_user(id);

CREATE TABLE shuup_companycontact ( \
  contact_ptr_id int NOT NULL, \
  tax_number int, \
  PRIMARY KEY(contact_ptr_id) \
);

CREATE TABLE shuup_companycontact_members ( \
  id int, \
  companycontact_id int NOT NULL, \
  ACCESSOR_contact_id int, \
  PRIMARY KEY (id), \
  FOREIGN KEY (companycontact_id) REFERENCES shuup_companycontact(contact_ptr_id), \
  FOREIGN KEY (ACCESSOR_contact_id) REFERENCES shuup_personcontact(pid) \
);

CREATE TABLE shuup_shop ( \
  id int, \
  OWNER_owner_id int NOT NULL, \
  PRIMARY KEY (id), \
  FOREIGN KEY (OWNER_owner_id) REFERENCES auth_user(id) \
);
CREATE INDEX shuup_shop_id ON shuup_shop(id);

CREATE TABLE shuup_gdpr_gdpruserconsent ( \
  id int, \
  created_on datetime, \
  shop_id int NOT NULL, \
  OWNER_user_id int NOT NULL, \
  FOREIGN KEY (shop_id) REFERENCES shuup_shop(id), \
  FOREIGN KEY (OWNER_user_id) REFERENCES auth_user(id), \
  PRIMARY KEY (id) \
);

EXPLAIN PRIVACY;