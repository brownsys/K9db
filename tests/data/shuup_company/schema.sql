CREATE TABLE shuup_contact ( \
  id int, \
  ptr int, \
  name text, \
  PRIMARY KEY(id) \
);

CREATE INDEX shuup_contact_ptr ON shuup_contact(ptr);

CREATE TABLE auth_user ( \
  id int, \
  ptr int, \
  username text, \
  password text, \
  PRIMARY KEY(id) \
);

CREATE INDEX auth_user_ptr ON auth_user(ptr);

CREATE TABLE shuup_personcontact ( \
  pid int, \
  OWNING_contact_ptr_id int, \
  PII_name text, \
  OWNING_user_id int, \
  email text, \
  PRIMARY KEY(pid), \
  FOREIGN KEY (OWNING_contact_ptr_id) REFERENCES shuup_contact(ptr), \
  FOREIGN KEY (OWNING_user_id) REFERENCES auth_user(ptr) \
);

CREATE TABLE shuup_companycontact ( \
  OWNER_contact_ptr_id int, \
  tax_number int, \
  FOREIGN KEY (OWNER_contact_ptr_id) REFERENCES shuup_contact(ptr), \
);

CREATE TABLE shuup_companycontact_members ( \
  id int, \
  ACCESSOR_ANONYMIZE_companycontact_id int, \
  OWNER_contact_id int, \
  FOREIGN KEY (ACCESSOR_ANONYMIZE_companycontact_id) REFERENCES shuup_companycontact(OWNER_contact_ptr_id), \
  FOREIGN KEY (OWNER_contact_id) REFERENCES shuup_personcontact(pid), \
);