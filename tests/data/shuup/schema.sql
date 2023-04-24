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
  contact_ptr_id int NOT NULL, \
  name text, \
  user_id int NOT NULL, \
  email text, \
  PRIMARY KEY(pid), \
  FOREIGN KEY (contact_ptr_id) OWNS shuup_contact(id), \
  FOREIGN KEY (user_id) OWNS auth_user(id) \
);

CREATE TABLE shuup_companycontact ( \
  contact_ptr_id int NOT NULL, \
  tax_number int, \
  PRIMARY KEY(contact_ptr_id), \
  FOREIGN KEY (contact_ptr_id) REFERENCES shuup_contact(id) \
);

CREATE TABLE shuup_companycontact_members ( \
  id int NOT NULL, \
  companycontact_id int NOT NULL, \
  contact_id int NOT NULL, \
  PRIMARY KEY (id), \
  FOREIGN KEY (companycontact_id) REFERENCES shuup_companycontact(contact_ptr_id), \
  FOREIGN KEY (contact_id) ACCESSED_BY shuup_personcontact(pid), \
  ON DEL contact_id DELETE_ROW \
);

CREATE TABLE shuup_shop ( \
  id int, \
  owner_id int NOT NULL, \
  PRIMARY KEY (id), \
  FOREIGN KEY (owner_id) OWNED_BY shuup_personcontact(pid) \
);

CREATE TABLE shuup_gdpr_gdpruserconsent ( \
  id int, \
  created_on datetime, \
  shop_id int NOT NULL, \
  user_id int NOT NULL, \
  FOREIGN KEY (shop_id) REFERENCES shuup_shop(id), \
  FOREIGN KEY (user_id) OWNED_BY auth_user(id), \
  PRIMARY KEY (id) \
);

CREATE TABLE shuup_mutaddress ( \
  id int, \
  tax_number int, \
  user_id int, \
  PRIMARY KEY (id), \
  FOREIGN KEY (user_id) ACCESSED_BY auth_user(id) \
);

CREATE TABLE shuup_basket ( \
  id int, \
  creator_id int, \
  customer_id int, \
  orderer_id int, \
  shop_id int, \
  PRIMARY KEY (id), \
  FOREIGN KEY (creator_id) REFERENCES auth_user(id), \
  FOREIGN KEY (customer_id) OWNED_BY shuup_personcontact(pid), \
  FOREIGN KEY (orderer_id) ACCESSED_BY shuup_personcontact(pid) \
);

CREATE TABLE shuup_order ( \
  id int, \
  creator_id int, \
  customer_id int, \
  modified_by_id int, \
  shop_id int, \
  orderer_id int, \
  account_manager_id int, \
  billing_address_id int, \
  PRIMARY KEY (id), \
  FOREIGN KEY (creator_id) OWNED_BY auth_user(id), \
  FOREIGN KEY (customer_id) OWNED_BY shuup_contact(id), \
  FOREIGN KEY (billing_address_id) OWNS shuup_mutaddress(id), \
  FOREIGN KEY (modified_by_id) ACCESSED_BY auth_user(id), \
  FOREIGN KEY (shop_id) ACCESSED_BY shuup_shop(id), \
  FOREIGN KEY (orderer_id) ACCESSED_BY shuup_personcontact(pid), \
  FOREIGN KEY (account_manager_id) ACCESSED_BY shuup_personcontact(pid) \
);
