CREATE TABLE shuup_personcontact ( \
  OWNS_contact_ptr_id int, \
  PII_name text, \
  OWNS_user_id int, \
  email text, \
  PRIMARY KEY(OWNS_contact_ptr_id) \
  FOREIGN KEY (OWNS_contact_ptr_id) REFERENCES shuup_contact(id), \
  FOREIGN KEY (OWNS_user_id) REFERENCES auth_user(id) \
);

CREATE TABLE shuup_contact ( \
  id int, \
  name text, \
  PRIMARY KEY(id) \
);

CREATE TABLE auth_user ( \
  id int, \
  username text, \
  password text, \
  PRIMARY KEY(id) \
);
