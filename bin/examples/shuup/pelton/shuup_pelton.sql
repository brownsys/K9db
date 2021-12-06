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
  -- TODO: customer is not actually the owner, since order data is retained in anoymized form
  OWNER_customer_id int, \ 
  shop_id int, \
  reference_number text, \
  -- anon
  phone text, \
  -- anon
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
  -- TODO: add foreign keys into mutableaddress and immutableaddress
);

CREATE TABLE "shuup_order"
             (
                          "id"               INTEGER NOT NULL PRIMARY KEY autoincrement,
                          "created_on"       datetime NOT NULL,
                          "modified_on"      datetime NOT NULL,
                          "identifier"       VARCHAR(64) NULL UNIQUE,
                          "label"            VARCHAR(32) NOT NULL,
                          "key"              VARCHAR(32) NOT NULL UNIQUE,
                          "reference_number" VARCHAR(64) NULL UNIQUE,
                          "phone"            VARCHAR(64) NOT NULL,
                          "email"            VARCHAR(128) NOT NULL,
                          "deleted" bool NOT NULL,
                          "payment_status"      INTEGER NOT NULL,
                          "shipping_status"     INTEGER NOT NULL,
                          "payment_method_name" VARCHAR(100) NOT NULL,
                          "payment_data" text NULL,
                          "shipping_method_name" VARCHAR(100) NOT NULL,
                          "shipping_data" text NULL,
                          "extra_data" text NULL,
                          "taxful_total_price_value"  DECIMAL NOT NULL,
                          "taxless_total_price_value" DECIMAL NOT NULL,
                          "currency"                  VARCHAR(4) NOT NULL,
                          "prices_include_tax" bool NOT NULL,
                          "display_currency"      VARCHAR(4) NOT NULL,
                          "display_currency_rate" DECIMAL NOT NULL,
                          "ip_address"            CHAR(39) NULL,
                          "order_date"            datetime NOT NULL,
                          "payment_date"          datetime NULL,
                          "language"              VARCHAR(10) NOT NULL,
                          "customer_comment" text NOT NULL,
                          "admin_comment" text NOT NULL,
                          "require_verification" bool NOT NULL,
                          "all_verified" bool NOT NULL,
                          "marketing_permission" bool NOT NULL,
                          "_codes" text NULL,
                          "billing_address_id"  INTEGER NULL REFERENCES "shuup_immutableaddress" ("id") deferrable initially deferred,
                          "creator_id"          INTEGER NULL REFERENCES "auth_user" ("id") deferrable initially deferred,
                          "customer_id"         INTEGER NULL REFERENCES "shuup_contact" ("id") deferrable initially deferred,
                          "modified_by_id"      INTEGER NULL REFERENCES "auth_user" ("id") deferrable initially deferred,
                          "payment_method_id"   INTEGER NULL REFERENCES "shuup_paymentmethod" ("id") deferrable initially deferred,
                          "shipping_address_id" INTEGER NULL REFERENCES "shuup_immutableaddress" ("id") deferrable initially deferred,
                          "shipping_method_id"  INTEGER NULL REFERENCES "shuup_shippingmethod" ("id") deferrable initially deferred,
                          "shop_id"             INTEGER NOT NULL REFERENCES "shuup_shop" ("id") deferrable initially deferred,
                          "status_id"           INTEGER NOT NULL REFERENCES "shuup_orderstatus" ("id") deferrable initially deferred,
                          "orderer_id"          INTEGER NULL REFERENCES "shuup_personcontact" ("contact_ptr_id") deferrable initially deferred,
                          "account_manager_id"  INTEGER NULL REFERENCES "shuup_personcontact" ("contact_ptr_id") deferrable initially deferred,
                          "tax_group_id"        INTEGER NULL REFERENCES "shuup_customertaxgroup" ("id") deferrable initially deferred,
                          "tax_number"          VARCHAR(64) NOT NULL
             )

-- note: only open orders are anonymized in the mutableaddress table. Previous orders are retained
-- in clear text form
CREATE TABLE "shuup_mutableaddress"
             (
                          "id"           INTEGER NOT NULL PRIMARY KEY autoincrement,
                          -- anon
                          "prefix"       VARCHAR(64) NOT NULL,
                          -- anon
                          "name"         VARCHAR(255) NOT NULL,
                          -- anon
                          "suffix"       VARCHAR(64) NOT NULL,
                          -- anon
                          "name_ext"     VARCHAR(255) NOT NULL,
                          -- anon
                          "company_name" VARCHAR(255) NOT NULL,
                          -- anon
                          "phone"        VARCHAR(64) NOT NULL,
                          -- anon
                          "email"        VARCHAR(128) NOT NULL,
                          -- anon
                          "street"       VARCHAR(255) NOT NULL,
                          -- anon
                          "street2"      VARCHAR(255) NOT NULL,
                          -- anon
                          "street3"      VARCHAR(255) NOT NULL,
                          -- anon
                          "postal_code"  VARCHAR(64) NOT NULL,
                          -- anon
                          "city"         VARCHAR(255) NOT NULL,
                          -- anon
                          "region_code"  VARCHAR(64) NOT NULL,
                          -- anon
                          "region"       VARCHAR(64) NOT NULL,
                          -- NOT anon
                          "country"      VARCHAR(2) NOT NULL,
                          -- anon
                          "longitude"    DECIMAL NULL,
                          -- anon
                          "latitude"     DECIMAL NULL,
                          -- NOT anon
                          "tax_number"   VARCHAR(64) NOT NULL
             )


CREATE TABLE "shuup_immutableaddress"
             (
                          "id"           INTEGER NOT NULL PRIMARY KEY autoincrement,
                          -- anon
                          "prefix"       VARCHAR(64) NOT NULL,
                          -- anon
                          "name"         VARCHAR(255) NOT NULL,
                          -- anon
                          "suffix"       VARCHAR(64) NOT NULL,
                          -- anon
                          "name_ext"     VARCHAR(255) NOT NULL,
                          -- anon
                          "company_name" VARCHAR(255) NOT NULL,
                          -- anon
                          "phone"        VARCHAR(64) NOT NULL,
                          -- anon
                          "email"        VARCHAR(128) NOT NULL,
                          -- anon
                          "street"       VARCHAR(255) NOT NULL,
                          -- anon
                          "street2"      VARCHAR(255) NOT NULL,
                          -- anon
                          "street3"      VARCHAR(255) NOT NULL,
                          -- anon
                          "postal_code"  VARCHAR(64) NOT NULL,
                          -- anon
                          "city"         VARCHAR(255) NOT NULL,
                          -- anon
                          "region_code"  VARCHAR(64) NOT NULL,
                          -- anon
                          "region"       VARCHAR(64) NOT NULL,
                          -- NOT anon
                          "country"      VARCHAR(2) NOT NULL,
                          -- anon
                          "longitude"    DECIMAL NULL,
                          -- anon
                          "latitude"     DECIMAL NULL,
                          -- NOT anon
                          "tax_number"   VARCHAR(64) NOT NULL
             )