-- # TODO:
-- open github bug report for shuup regarded 
-- 1. anonymization button anonymizes everyone not just single user as shown
-- 2. mutaddress rows associated with completed orders are not anonymized, only outstanding orders are anonymized
-- 3. user download data button doesn't return any data
-- 4. cookies active by default?
-- 5. shuup doesn't delete any data, only anonymizes it 

-- # TODO: paper text on shuup
-- expressiveness, no overhead, class of applications in e-commerce where all data needs
-- to be retained for tax purposes
-- our annotations support this with no overhead bc all data is retained, so no microDBs/sharding needed

-- benchmarking: cost of accessor annotations on insert
-- quickly benchmark GDPR GET 
-- in future can run script to fill with fake data to see

CREATE TABLE auth_users ( \
  id int, \
  password text, \
  last_login datetime, \
  is_superuser int, \
  username text, \
  PII_first_name text, \
  email text, \
  is_staff int, \
  is_active int, \
  date_joined datetime, \
  last_name text, \
  PRIMARY KEY(id) \
);

-- table that can be deleted (sharded)
CREATE TABLE shuup_basket ( \
  id int, \
  key text, \
  created_on datetime, \
  updated_on datetime, \
  persistent int, \
  deleted int, \
  finished int, \
  title text, \
  data text, \
  taxless_total_price_value text, \
  taxful_total_price_value text, \
  currency text, \
  prices_include_tax int, \
  product_count int, \
  OWNER_creator_id int, \
  customer_id int, \
  shop_id int, \
  orderer_id int, \
  PRIMARY KEY (id), \
  FOREIGN KEY (creator_id) REFERENCES auth_users(id), \
  FOREIGN KEY (customer_id) REFERENCES shuup_contact(id) \
);

-- table that can be deleted (sharded)
CREATE TABLE shuup_gdpr_gdpruserconsent ( \
  id int, \
  created_on datetime, \
  shop_id int, \
  OWNER_user_id int, \
  FOREIGN KEY (user_id) REFERENCES auth_users(id) \
);

-- table that can be deleted (sharded)
CREATE TABLE shuup_personcontact ( \
  contact_ptr_id int, \
  gender datetime, \
  birth_date datetime, \
  first_name text, \
  last_name int, \
  OWNER_user_id text, \
  PRIMARY KEY (contact_ptr_id), \
  FOREIGN KEY (user_id) REFERENCES auth_users(id) \
);

-- ?
CREATE TABLE shuup_contact ( \
  id int, \
  created_on datetime, \
  modified_on datetime, \
  identifier text, \
  is_active int, \
  _language text, \
  marketing_permission text, \
  phone int, \
  www text, \
  timezone text, \
  prefix text, \
  name text, \
  name_ext text, \
  email text, \
  merchant_notes text, \
  default_billing_address_id int, \
  default_payment_method_id int, \
  default_shipping_address_id int, \
  default_shipping_method_id int, \
  account_manager_id int, \
  registration_shop_id int, \
  options text, \
  picture_id int, \
  tax_group_id int, \
  PRIMARY KEY (id), \
  FOREIGN KEY (account_manager_id) REFERENCES shuup_personcontact(contact_ptr_id), \
  FOREIGN KEY (default_billing_address_id) REFERENCES shuup_mutableaddress(id), \
  FOREIGN KEY (default_shipping_address_id) REFERENCES shuup_mutableaddress(id), \
);

-- anonymized, retained
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
  ACCESSOR_ANONYMIZE_user_id int, \
  PRIMARY KEY (id), \
  FOREIGN KEY (ACCESSOR_ANONYMIZE_user_id) REFERENCES auth_users(id) \
);

-- anonymized, retained
CREATE TABLE shuup_imaddress ( \
  id int, \
  ANONYMIZE_prefix text, \
  ANONYMIZE_name text, \
  ANONYMIZE_suffix text, \
  ANONYMIZE_name_ext text, \
  ANONYMIZE_company_name text, \
  ANONYMIZE_phone text, \
  ANONYMIZE_email text, \
  ANONYMIZE_street text, \
  ANONYMIZE_street2 text, \
  ANONYMIZE_street3 text, \
  ANONYMIZE_postal_code text, \
  ANONYMIZE_city text, \
  region_code text, \
  region text, \
  country text, \
  tax_number text, \
  ACCESSOR_ANONYMIZE_user_id int, \
  PRIMARY KEY (id), \
  FOREIGN KEY (ACCESSOR_ANONYMIZE_user_id) REFERENCES auth_users(id) \
);

-- anonymized, retained
CREATE TABLE shuup_order ( \
  id int, \
  ACCESSOR_ANONYMIZE_customer_id int, \
  shop_id int, \
  reference_number text, \
  ANONYMIZE_phone text, \
  ANONYMIZE_email text, \
  deleted int, \
  payment_status int, \
  shipping_status int, \
  payment_method_name text, \
  taxful_total_price_value int, \
  currency text, \
  order_date datetime, \
  payment_date datetime, \
  billing_address_id int, \
  shipping_address_id int, \
  PRIMARY KEY (id), \
  FOREIGN KEY (ACCESSOR_ANONYMIZE_customer_id) REFERENCES auth_users(id), \
  FOREIGN KEY (billing_address_id) REFERENCES shuup_immutableaddress(id), \
  FOREIGN KEY (shipping_address_id) REFERENCES shuup_immutableaddress(id) \
);

INSERT INTO auth_users VALUES (1, 'password', '2021-04-21 01:00:00', 0, 'user1', 'banjy', 'banjy@evil.com', 0, 1, '2021-04-21 01:00:00', 'evil');
INSERT INTO shuup_imaddress VALUES (1, 'prefix', 'name', 'suffix', 'name_ext', 'company_name', '401-401-4010', 'banjy@evil.com', 'street1', 'street2', 'street3', '02912', 'providence', 'NA', 'North America', 'USA', 'tax 123', 1);
INSERT INTO shuup_mutaddress VALUES (1, 'prefix', 'name', 'suffix', 'name_ext', 'company_name', '401-401-4010', 'banjy@evil.com', 'street1', 'street2', 'street3', '02912', 'providence', 'NA', 'North America', 'USA', 'tax 123', 1);
INSERT INTO shuup_order VALUES (1, 1, 5, 'reference_number', '401-401-4010', 'banjy@evil.com', 0, 1, 0, 'cash', 100, 'USD', '2021-04-21 01:00:00', '2021-04-21 01:00:00', 1, 1);

GDPR GET auth_users 1;
GDPR FORGET auth_users 1;
SELECT * FROM auth_users;
SELECT * FROM shuup_imaddress;
SELECT * FROM shuup_mutaddress;
SELECT * FROM shuup_order;
