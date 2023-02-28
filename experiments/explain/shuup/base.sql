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
  OWNING_contact_ptr_id int, \
  PII_name text, \
  OWNING_user_id int, \
  email text, \
  PRIMARY KEY(pid), \
  FOREIGN KEY (OWNING_contact_ptr_id) REFERENCES shuup_contact(ptr), \
  FOREIGN KEY (OWNING_user_id) REFERENCES auth_user(ptr) \
);

CREATE TABLE shuup_companycontact ( \
  contact_ptr_id int, \
  tax_number int, \
  PRIMARY KEY(contact_ptr_id) \
);

CREATE TABLE shuup_companycontact_members ( \
  id int, \
  companycontact_id int, \
  ACCESSOR_contact_id int, \
  PRIMARY KEY (id), \
  FOREIGN KEY (companycontact_id) REFERENCES shuup_companycontact(contact_ptr_id), \
  FOREIGN KEY (ACCESSOR_contact_id) REFERENCES shuup_personcontact(pid) \
);

CREATE TABLE shuup_shop ( \
  id int, \
  OWNER_owner_id int, \
  PRIMARY KEY (id), \
  FOREIGN KEY (OWNER_owner_id) REFERENCES auth_user(id) \
);

CREATE TABLE shuup_gdpr_gdpruserconsent ( \
  id int, \
  created_on datetime, \
  shop_id int, \
  OWNER_user_id int, \
  FOREIGN KEY (shop_id) REFERENCES shuup_shop(id), \
  FOREIGN KEY (OWNER_user_id) REFERENCES auth_user(id), \
  PRIMARY KEY (id) \
);

-- todo: work out functionality of default address. Deletion behavior. 
-- OWNS on default will only delete/anon default address. So don't use OWNS
CREATE TABLE shuup_mutaddress ( \
  id int, \
  prefix text, \
  name text, \
  suffix text, \
  name_ext text, \
  company_name text, \
  phone text, \
  email text, \
  street text, \
  street2 text, \
  street3 text, \
  postal_code text, \
  city text, \
  region_code text, \
  region text, \
  country text, \
  tax_number text, \
  -- if needs to stay behind, accessor anon. If deleted, use OWNER (owned by)
  ACCESSOR_ANONYMIZE_user_id int, \
  PRIMARY KEY (id), \
  FOREIGN KEY (ACCESSOR_ANONYMIZE_user_id) REFERENCES auth_users(id) \
);

-- take bad scenarios and run them thru explain to get useful warnings. 
-- narrative, starting with schema that looks reasonable and find that data is duplicated, then iteratively improve
-- on it using explain privacy as guidance.

CREATE VIEW shuup_contact_owned_by_shuup_personcontact AS '"SELECT OWNING_contact_ptr_id, pid, COUNT(*) FROM shuup_personcontact GROUP BY OWNING_contact_ptr_id, pid HAVING OWNING_contact_ptr_id = ?"';
CREATE VIEW auth_user_owned_by_shuup_personcontact AS '"SELECT OWNING_user_id, pid, COUNT(*) FROM shuup_personcontact GROUP BY OWNING_user_id, pid HAVING OWNING_user_id = ?"';

CREATE VIEW auth_personcontact AS 
'"SELECT auth_user.id, shuup_personcontact.pid, COUNT(*)
FROM auth_user 
JOIN shuup_personcontact ON auth_user.ptr = shuup_personcontact.OWNING_user_id
WHERE auth_user.id = ?
GROUP BY (auth_user.id, shuup_personcontact.pid)
HAVING COUNT(*) > 0"' ;

INSERT INTO shuup_personcontact(pid, OWNING_contact_ptr_id, PII_name, OWNING_user_id, email) VALUES (1, 100, 'Alice', 10, 'email@email.com');
INSERT INTO shuup_contact(id, ptr, name) VALUES (6, 100, 'Alice');
INSERT INTO auth_user(id, ptr, username, password) VALUES (15, 10, 'Alice', 'secure');

INSERT INTO shuup_companycontact(contact_ptr_id, tax_number) VALUES (100, 6666);
INSERT INTO shuup_companycontact_members(id, companycontact_id, ACCESSOR_contact_id) VALUES (1, 100, 1);

INSERT INTO shuup_shop(id, OWNER_owner_id) VALUES (1, 15);
INSERT INTO shuup_gdpr_gdpruserconsent(id, created_on, shop_id, OWNER_user_id) VALUES (1, "00:00:00", 1, 15);

-- fix for shuup_contact and gdprconsent transivity issue: need an indx that joins personcontact & authuser. Maps auth_user(id) to shuup_personcontact(pid)
-- problem is that pelton doesn't check if the col pointed to is what we are sharding by. Index doesn't point all the way to personcontact
CREATE VIEW gdpr_to_personcontact AS 
'"SELECT shuup_shop.id, shuup_personcontact.pid, COUNT(*)
FROM auth_user 
JOIN shuup_personcontact ON auth_user.ptr = shuup_personcontact.OWNING_user_id
JOIN shuup_shop ON auth_user.id = shuup_shop.OWNER_owner_id
WHERE shuup_shop.id = ?
GROUP BY (shuup_shop.id, shuup_personcontact.pid)
HAVING COUNT(*) > 0"' ;

-- todo: issue that indices not created. On resharding for var owned table (maybe if only PII)
-- during resharding, the indices for the PKs don't get created. 
-- shuup example that created the error

-- let justus know when around lab
-- bug 1: gdpr get doesn't lookup manually created views/indices
-- bug 2: accessor doesn't work
-- bug 3: rocksdb index not sharded, so pelton doesn't see it as an index

-- in log statements showing which shard inserted into the foreign key is prepended to the table name e.g. shuup_contact_OWNING_contact_ptr_id

-- any PII_ annotation on a col in a table means that we shard by the primary key of that table

-- todo:
-- new annotation, ignore_. So that pelton doesn't shard it. Check for that ignore in create.cc
-- act is if not foreign key

-- indirect way to see what pelton does. Explain privacy might fix this by looking at the metadata 
-- shows what pelton thinks the layout of the data is (where, in what shards data is stored in)

-- won't . do select * from the table in question and look at output. 
-- all data of same user should be contiguous. Everything that belongs to one user will 

-- because indices created manually, pelton doesn't realise these are things it needs to lookup in gdpr get

-- goal for explain privacy is to handle in a concrete way the broken shuup schemas
--- detect duplicated data (the false annotations that we discussed in the beginning - i.e. making
--- shuup_contact or auth_user the data subject, which is either wrong or leads to duplicate data)

-- todo: gdpr get. fix it for this
--todo: explain privacy (justus)
