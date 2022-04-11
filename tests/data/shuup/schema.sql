-- CREATE TABLE shuup_contact ( \
--   id int, \
--   ptr int, \
--   name text, \
--   PRIMARY KEY(id) \
-- );

-- CREATE INDEX shuup_contact_ptr ON shuup_contact(ptr);

-- CREATE TABLE auth_user ( \
--   id int, \
--   ptr int, \
--   username text, \
--   password text, \
--   PRIMARY KEY(id) \
-- );

-- CREATE INDEX auth_user_ptr ON auth_user(ptr);

-- CREATE TABLE shuup_personcontact ( \
--   pid int, \
--   OWNING_contact_ptr_id int, \
--   PII_name text, \
--   OWNING_user_id int, \
--   email text, \
--   PRIMARY KEY(pid), \
--   FOREIGN KEY (OWNING_contact_ptr_id) REFERENCES shuup_contact(ptr), \
--   FOREIGN KEY (OWNING_user_id) REFERENCES auth_user(ptr) \
-- );

CREATE TABLE shuup_contact ( \
  id int, \
  ptr int, \
  name text, \
  PRIMARY KEY(id) \
);
-- index here: rocksdb (not created, since not sharded yet). 
-- Both PII and OWNING causes trouble:
CREATE TABLE shuup_personcontact ( \
  pid int, \
  OWNING_contact_ptr_id int, \
  PII_name text, \
  email text, \
  PRIMARY KEY(pid), \
  FOREIGN KEY (OWNING_contact_ptr_id) REFERENCES shuup_contact(ptr) \
);

-- index here: pelton 
-- CREATE INDEX shuup_contact_ptr ON shuup_contact(ptr);
CREATE VIEW owned_by_shuup_personcontact AS '"SELECT OWNING_contact_ptr_id, pid, COUNT(*) FROM shuup_personcontact GROUP BY OWNING_contact_ptr_id, pid HAVING OWNING_contact_ptr_id = ?"';
-- two indices for auth_user, change name. 
INSERT INTO shuup_personcontact VALUES (1, 100, 'Alice', 'email@email.com');
INSERT INTO shuup_contact(id, ptr, name) VALUES (6, 100, 'Alice');