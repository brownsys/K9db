CREATE TABLE auth_user ( \
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
  PRIMARY KEY(id) \
);

CREATE INDEX shuup_contact_ptr ON shuup_contact(ptr);

CREATE TABLE shuup_personcontact ( \
  pid int, \
  contact_ptr_id int, \
  name text, \
  user_id int, \
  email text, \
  PRIMARY KEY(pid), \
  FOREIGN KEY (contact_ptr_id) REFERENCES shuup_contact(ptr), \
  FOREIGN KEY (user_id) REFERENCES auth_user(ptr) \
);

CREATE INDEX auth_user_ptr ON auth_user(id);

CREATE TABLE shuup_companycontact ( \
  contact_ptr_id int, \
  tax_number int, \
  PRIMARY KEY(contact_ptr_id) \
);

CREATE TABLE shuup_companycontact_members ( \
  id int, \
  companycontact_id int, \
  contact_id int, \
  PRIMARY KEY (id), \
  FOREIGN KEY (companycontact_id) REFERENCES shuup_companycontact(contact_ptr_id), \
  FOREIGN KEY (contact_id) REFERENCES shuup_personcontact(pid) \
);

CREATE TABLE shuup_shop ( \
  id int, \
  owner_id int, \
  PRIMARY KEY (id), \
  FOREIGN KEY (owner_id) REFERENCES auth_user(id) \
);
CREATE INDEX shuup_shop_id ON shuup_shop(id);

CREATE TABLE shuup_gdpr_gdpruserconsent ( \
  id int, \
  created_on datetime, \
  shop_id int, \
  user_id int, \
  FOREIGN KEY (shop_id) REFERENCES shuup_shop(id), \
  FOREIGN KEY (user_id) REFERENCES auth_user(id), \
  PRIMARY KEY (id) \
);

EXPLAIN PRIVACY;