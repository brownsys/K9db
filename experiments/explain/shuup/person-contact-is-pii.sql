CREATE TABLE auth_user ( \
  id int NOT NULL, \
  username text, \
  password text, \
  PRIMARY KEY(id) \
);

CREATE TABLE shuup_contact ( \
  id int NOT NULL, \
  name text, \
  email text, \
  PRIMARY KEY(id) \
);

CREATE DATA_SUBJECT TABLE shuup_personcontact ( \
  pid int NOT NULL, \
  contact_ptr_id int NOT NULL OWNS shuup_contact(id), \
  name text, \
  user_id int NOT NULL OWNS auth_user(id), \
  PRIMARY KEY(pid) \
);

CREATE TABLE shuup_companycontact ( \
  contact_ptr_id int NOT NULL REFERENCES shuup_contact(id), \
  tax_number int, \
  PRIMARY KEY(contact_ptr_id) \
);

CREATE TABLE shuup_companycontact_members ( \
  id int NOT NULL, \
  companycontact_id int NOT NULL, \
  contact_id int NOT NULL OWNED_BY shuup_personcontact(pid), \
  PRIMARY KEY (id), \
  FOREIGN KEY (companycontact_id) REFERENCES shuup_companycontact(contact_ptr_id) \
);

CREATE TABLE shuup_shop ( \
  id int, \
  owner_id int NOT NULL OWNED_BY shuup_personcontact(pid), \
  PRIMARY KEY (id) \
);

CREATE TABLE shuup_gdpr_gdpruserconsent ( \
  id int, \
  created_on datetime, \
  shop_id int NOT NULL, \
  user_id int NOT NULL OWNED_BY shuup_personcontact(pid), \
  FOREIGN KEY (shop_id) REFERENCES shuup_shop(id), \
  PRIMARY KEY (id) \
);

EXPLAIN COMPLIANCE;
