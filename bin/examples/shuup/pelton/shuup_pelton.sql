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
  ACCESSOR_user_id int, \
  PRIMARY KEY (id), \
  FOREIGN KEY (ACCESSOR_user_id) REFERENCES auth_users(id) \
);

CREATE INDEX shuup_mutaddress_index ON shuup_mutaddress (ACCESSOR_user_id);

CREATE TABLE shuup_imaddress ( \
  id int, \
  ACCESS_prefix text, \
  ACCESS_name text, \
  ACCESS_suffix text, \
  ACCESS_name_ext text, \
  ACCESS_company_name text, \
  ACCESS_phone text, \
  ACCESS_email text, \
  ACCESS_street text, \
  ACCESS_street2 text, \
  ACCESS_street3 text, \
  ACCESS_postal_code text, \
  ACCESS_city text, \
  ACCESS_region_code text, \
  ACCESS_region text, \
  country text, \
  tax_number text, \
  ACCESSOR_user_id int, \
  PRIMARY KEY (id), \
  FOREIGN KEY (ACCESSOR_user_id) REFERENCES auth_users(id) \
);

CREATE INDEX shuup_imaddress_index ON shuup_imaddress (ACCESSOR_user_id);

CREATE TABLE shuup_order ( \
  id int, \
  ACCESSOR_customer_id int, \
  shop_id int, \
  reference_number text, \
  ACCESS_phone text, \
  ACCESS_email text, \
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
  FOREIGN KEY (ACCESSOR_customer_id) REFERENCES auth_users(id), \
  FOREIGN KEY (billing_address_id) REFERENCES shuup_immutableaddress(id), \
  FOREIGN KEY (shipping_address_id) REFERENCES shuup_immutableaddress(id) \
);

INSERT INTO auth_users VALUES (1, 'password', '2021-04-21 01:00:00', 0, 'user1', 'banjy', 'banjy@evil.com', 0, 1, '2021-04-21 01:00:00', 'evil');
INSERT INTO shuup_imaddress VALUES (1, 'prefix', 'name', 'suffix', 'name_ext', 'company_name', '401-401-4010', 'banjy@evil.com', 'street1', 'street2', 'street3', '02912', 'providence', 'NA', 'North America', 'USA', 'tax 123', 1);
INSERT INTO shuup_mutaddress VALUES (1, 'prefix', 'name', 'suffix', 'name_ext', 'company_name', '401-401-4010', 'banjy@evil.com', 'street1', 'street2', 'street3', '02912', 'providence', 'NA', 'North America', 'USA', 'tax 123', 1);
INSERT INTO shuup_order VALUES (1, 1, 5, 'reference_number', '401-401-4010', 'banjy@evil.com', 0, 1, 0, 'cash', 100, 'USD', '2021-04-21 01:00:00', '2021-04-21 01:00:00', 1, 1);

CREATE TABLE shuup_payment ( \
  id int, \
  order_id int, \
  created_on datetime, \
  gateway_id text, \
  payment_identifier text, \
  amount_value int, \
  description text, \
  PRIMARY KEY (id), \
  FOREIGN KEY (order_id) REFERENCES shuup_order(id) \
);


