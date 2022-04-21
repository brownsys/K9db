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
  PII_name text, \
  email text, \
  PRIMARY KEY(id) \
);


CREATE TABLE shuup_personcontact ( \
  pid int, \
  OWNING_contact_ptr_id int NOT NULL REFERENCES shuup_contact(ptr), \
  name text, \
  OWNER_user_id int REFERENCES auth_user(ptr), \
  PRIMARY KEY(pid) \
);

CREATE INDEX personcontact_ptr ON shuup_personcontact(pid);

CREATE TABLE shuup_companycontact ( \
  contact_ptr_id int NOT NULL REFERENCES shuup_contact(ptr), \
  tax_number int, \
  PRIMARY KEY(contact_ptr_id) \
);


CREATE TABLE shuup_companycontact_members ( \
  id int, \
  companycontact_id int NOT NULL, \
  OWNER_contact_id int REFERENCES shuup_personcontact(pid), \
  PRIMARY KEY (id), \
  FOREIGN KEY (companycontact_id) REFERENCES shuup_companycontact(contact_ptr_id) \
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