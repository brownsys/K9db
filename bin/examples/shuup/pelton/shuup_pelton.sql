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
  OWNER_customer_id int, \
  shop_id int, \
  reference_number text, \
  phone text, \
  email text, \
  deleted int, \
  payment_status int, \
  shipping_status int, \
  payment_method_name text, \
  taxful_total_price_value int, \
  currency text, \
  order_date datetime, \
  payment_date datetime, \
  PRIMARY KEY (id), \
  FOREIGN KEY (OWNER_customer_id) REFERENCES auth_users(id) \
);

CREATE TABLE shuup_payment ( \
  id int, \
  order_id int, \
  created_on datetime, \
  gateway_id text, \
  payment_identifier text, \
  amount_value int, \
  description text, \
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
  FOREIGN KEY (target_id) REFERENCES shuup_payment(id) \
  FOREIGN KEY (ACCESSOR_user_id) REFERENCES auth_users(id) \
);