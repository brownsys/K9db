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

CREATE TABLE shuup_basket ( \
  id int, \
  basket_key text, \
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
  FOREIGN KEY (OWNER_creator_id) REFERENCES auth_users(id), \
  FOREIGN KEY (customer_id) REFERENCES shuup_contact(id) \
);

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
  customer_id int, \
  PRIMARY KEY (id), \
  FOREIGN KEY (ACCESSOR_ANONYMIZE_customer_id) REFERENCES auth_users(id), \
  FOREIGN KEY (billing_address_id) REFERENCES shuup_immutableaddress(id), \
  FOREIGN KEY (shipping_address_id) REFERENCES shuup_immutableaddress(id) \
  FOREIGN KEY (customer_id) REFERENCES shuup_contact(id) \
);