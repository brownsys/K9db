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

CREATE INDEX auth_user_ptr ON auth_user(id);

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
CREATE INDEX shuup_shop_id ON shuup_shop(id);

CREATE TABLE shuup_gdpr_gdpruserconsent ( \
  id int, \
  created_on datetime, \
  shop_id int, \
  OWNER_user_id int, \
  FOREIGN KEY (shop_id) REFERENCES shuup_shop(id), \
  FOREIGN KEY (OWNER_user_id) REFERENCES auth_user(id), \
  PRIMARY KEY (id) \
);

CREATE TABLE shuup_mutaddress ( \
  id int, \
  tax_number text, \
  ACCESSOR_ANONYMIZE_user_id int, \
  PRIMARY KEY (id), \
  FOREIGN KEY (ACCESSOR_ANONYMIZE_user_id) REFERENCES auth_user(id) \
);

CREATE TABLE shuup_basket ( \
  id int, \
  creator_id int, \
  OWNER_customer_id int, \
  ACCESSOR_ANONYMIZE_orderer_id int, \
  shop_id int, \
  PRIMARY KEY (id), \
  FOREIGN KEY (creator_id) REFERENCES auth_users(id), \
  FOREIGN KEY (OWNER_customer_id) REFERENCES shuup_personcontact(id), \
  FOREIGN KEY (ACCESSOR_ANONYMIZE_orderer_id) REFERENCES shuup_personcontact(id) \
);

CREATE TABLE shuup_order ( \
  id int, \
  OWNER_creator_id int, \
  OWNER_customer_id int, \
  ACCESSOR_modified_by_id int, \
  ACCESSOR_shop_id int, \
  ACCESSOR_ANONYMIZE_orderer_id int,
  account_manager_id int, \
  OWNS_billing_address_id int, \
  PRIMARY KEY (id), \
  FOREIGN KEY (OWNER_creator_id) REFERENCES auth_user(id), \
  FOREIGN KEY (OWNER_customer_id) REFERENCES shuup_contact(id), \
  FOREIGN KEY (ACCESSOR_modified_by_id) REFERENCES auth_user(id), \
  FOREIGN KEY (ACCESSOR_shop_id) REFERENCES shuup_shop(id), \
  FOREIGN KEY (ACCESSOR_ANONYMIZE_orderer_id) REFERENCES shuup_personcontact(id) \
);
-- Github Issue #130
  -- FOREIGN KEY (ACCESSOR_account_manager_id) REFERENCES shuup_personcontact(id) \

CREATE VIEW shuup_contact_owned_by_shuup_personcontact AS '"SELECT OWNING_contact_ptr_id, pid, COUNT(*) FROM shuup_personcontact GROUP BY OWNING_contact_ptr_id, pid HAVING OWNING_contact_ptr_id = ?"';
CREATE VIEW auth_user_owned_by_shuup_personcontact AS '"SELECT OWNING_user_id, pid, COUNT(*) FROM shuup_personcontact GROUP BY OWNING_user_id, pid HAVING OWNING_user_id = ?"';

CREATE VIEW auth_personcontact AS 
'"SELECT auth_user.id, shuup_personcontact.pid, COUNT(*)
FROM auth_user 
JOIN shuup_personcontact ON auth_user.ptr = shuup_personcontact.OWNING_user_id
WHERE auth_user.id = ?
GROUP BY (auth_user.id, shuup_personcontact.pid)
HAVING COUNT(*) > 0"' ;

CREATE VIEW contact_personcontact AS 
'"SELECT shuup_contact.id, shuup_personcontact.pid, COUNT(*)
FROM shuup_contact 
JOIN shuup_personcontact ON shuup_contact.ptr = shuup_personcontact.OWNING_contact_ptr_id
WHERE shuup_contact.id = ?
GROUP BY (shuup_contact.id, shuup_personcontact.pid)
HAVING COUNT(*) > 0"' ;

INSERT INTO shuup_personcontact(pid, OWNING_contact_ptr_id, PII_name, OWNING_user_id, email) VALUES (1, 100, 'Alice', 10, 'email@email.com');
INSERT INTO shuup_contact(id, ptr, name) VALUES (6, 100, 'Alice');
INSERT INTO auth_user(id, ptr, username, password) VALUES (15, 10, 'Alice', 'secure');

INSERT INTO shuup_personcontact(pid, OWNING_contact_ptr_id, PII_name, OWNING_user_id, email) VALUES (2, 200, 'Joe', 20, 'email@email.com');
INSERT INTO shuup_contact(id, ptr, name) VALUES (7, 200, 'Joe');
INSERT INTO auth_user(id, ptr, username, password) VALUES (16, 20, 'Joe', 'insecure');

INSERT INTO shuup_companycontact(contact_ptr_id, tax_number) VALUES (100, 6666);
INSERT INTO shuup_companycontact_members(id, companycontact_id, ACCESSOR_contact_id) VALUES (1, 100, 1);

INSERT INTO shuup_shop(id, OWNER_owner_id) VALUES (1, 15);
INSERT INTO shuup_gdpr_gdpruserconsent(id, created_on, shop_id, OWNER_user_id) VALUES (1, "00:00:00", 1, 15);

INSERT INTO shuup_mutaddress(id, tax_number, ACCESSOR_ANONYMIZE_user_id) VALUES (1, 6666, 15);

INSERT INTO shuup_basket(id, creator_id, OWNER_customer_id, ACCESSOR_ANONYMIZE_orderer_id, shop_id) VALUES (1, 15, 1, 2, 1);

INSERT INTO shuup_order(id, OWNER_creator_id, OWNER_customer_id, ACCESSOR_modified_by_id, ACCESSOR_shop_id, ACCESSOR_ANONYMIZE_orderer_id, account_manager_id, OWNS_billing_address_id) VALUES (1, 15, 6, 15, 1, 1, 2, 666);

CREATE VIEW gdpr_to_personcontact AS 
'"SELECT shuup_shop.id, shuup_personcontact.pid, COUNT(*)
FROM auth_user 
JOIN shuup_personcontact ON auth_user.ptr = shuup_personcontact.OWNING_user_id
JOIN shuup_shop ON auth_user.id = shuup_shop.OWNER_owner_id
WHERE shuup_shop.id = ?
GROUP BY (shuup_shop.id, shuup_personcontact.pid)
HAVING COUNT(*) > 0"' ;

GDPR GET shuup_personcontact 1;