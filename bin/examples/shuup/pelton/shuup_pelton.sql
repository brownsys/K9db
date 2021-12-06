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
  FOREIGN KEY (ACCESSOR_customer_id) REFERENCES auth_users(id) \
  FOREIGN KEY (ACCESSOR_customer_id) REFERENCES shuup_immutableaddress(id) \
  FOREIGN KEY (ACCESSOR_customer_id) REFERENCES shuup_immutableaddress(id) \
);

CREATE TABLE shuup_payment ( \
  id int, \
  order_id int, \
  created_on datetime, \
  gateway_id text, \
  payment_identifier text, \
  amount_value int, \
  description text, \
  PRIMARY KEY (id) \
  FOREIGN KEY (order_id) REFERENCES shuup_order(id) \
);

CREATE TABLE shuup_paymentlogentry ( \
  id int, \
  target_id int, \
  ACCESSOR_user_id int, \
  created_on datetime, \
  kind int, \
  identifier text, \
  message text, \
  PRIMARY KEY (id) \
  FOREIGN KEY (target_id) REFERENCES shuup_payment(id) \
  FOREIGN KEY (ACCESSOR_user_id) REFERENCES auth_users(id) \
);

CREATE TABLE shuup_mutableaddress ( \
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
  ACCESSOR_user_id int, 
  PRIMARY KEY (id) \
  FOREIGN KEY (ACCESSOR_user_id) REFERENCES auth_users(id) \
);

CREATE TABLE shuup_immutableaddress ( \
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
  ACCESSOR_user_id int, 
  PRIMARY KEY (id) \
  FOREIGN KEY (ACCESSOR_user_id) REFERENCES auth_users(id) \
);
