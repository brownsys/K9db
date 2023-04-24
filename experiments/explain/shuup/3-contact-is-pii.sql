CREATE TABLE auth_user ( \
  id int, \
  ptr int, \
  username text, \
  password text, \
  PRIMARY KEY(id) \
);

CREATE DATA_SUBJECT TABLE shuup_contact ( \
  id int, \
  name text, \
  email text, \
  PRIMARY KEY(id) \
);

CREATE TABLE shuup_personcontact ( \
  pid int, \
  contact_ptr_id int NOT NULL, \
  name text, \
  user_id int, \
  email text, \
  PRIMARY KEY(pid), \
  FOREIGN KEY (contact_ptr_id) REFERENCES shuup_contact(id), \
  FOREIGN KEY (user_id) OWNS auth_user(id) \
);

CREATE TABLE shuup_companycontact ( \
  id int PRIMARY KEY,
  contact_ptr_id int NOT NULL, \
  tax_number int, \
  FOREIGN KEY (contact_ptr_id) OWNED_BY shuup_contact(id) \
);

CREATE TABLE shuup_companycontact_members ( \
  id int, \
  companycontact_id int NOT NULL, \
  contact_id int, \
  PRIMARY KEY (id), \
  FOREIGN KEY (companycontact_id) REFERENCES shuup_companycontact(id), \
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
  shop_id int, \
  user_id int NOT NULL, \
  FOREIGN KEY (shop_id) REFERENCES shuup_shop(id), \
  FOREIGN KEY (user_id) OWNED_BY auth_user(id), \
  PRIMARY KEY (id) \
);

EXPLAIN COMPLIANCE;
