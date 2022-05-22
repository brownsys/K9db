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
  ACCESSOR_ANONYMIZE_orderer_id int, \
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