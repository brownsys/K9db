-- not supported
-- multi-col primary keys, UNIQUE KEY / KEY
-- invalid primary key, when PK annotation missing
-- columns named "match" "index" "key" "indexed" and "from" (probably reserved words)

CREATE TABLE PREFIX_accessory (
  id_product_1 int NOT NULL,
  id_product_2 int NOT NULL,
  PRIMARY KEY (id_product_1)
  -- KEY accessory_product (id_product_1, id_product_2)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Address info associated with a user */
CREATE TABLE PREFIX_address (
  id_address int NOT NULL,
  id_country int NOT NULL,
  id_state int,
  id_customer int NOT NULL,
  id_manufacturer int NOT NULL,
  id_supplier int NOT NULL,
  id_warehouse int NOT NULL,
  alias text NOT NULL,
  company text,
  lastname text NOT NULL,
  firstname text NOT NULL,
  address1 text NOT NULL,
  address2 text,
  postcode text,
  city text NOT NULL,
  other text,
  phone text,
  phone_mobile text,
  vat_number text,
  dni text,
  date_add datetime NOT NULL,
  date_upd datetime NOT NULL,
  active int NOT NULL,
  deleted int NOT NULL,
  PRIMARY KEY (id_address)
  -- KEY address_customer (id_customer),
  -- KEY id_country (id_country),
  -- KEY id_state (id_state),
  -- KEY id_manufacturer (id_manufacturer),
  -- KEY id_supplier (id_supplier),
  -- KEY id_warehouse (id_warehouse)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Used for search, if a search string is present inside the table, search the alias as well */
CREATE TABLE PREFIX_alias (
  id_alias int NOT NULL,
  alias text NOT NULL,
  search text NOT NULL,
  active int NOT NULL,
  PRIMARY KEY (id_alias)
  -- UNIQUE KEY alias (alias)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Contains all virtual products (attachements, like images, files, ...) */
CREATE TABLE PREFIX_attachment (
  id_attachment int NOT NULL,
  file text NOT NULL,
  file_name text NOT NULL,
  file_size int NOT NULL,
  mime text NOT NULL,
  PRIMARY KEY (id_attachment)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Name / Description linked to an attachment, localised */
CREATE TABLE PREFIX_attachment_lang (
  id_attachment int NOT NULL,
  id_lang int NOT NULL,
  name text,
  description text,
  PRIMARY KEY (id_attachment)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Relationship between a product and an attachment */
CREATE TABLE PREFIX_product_attachment (
  id_product int NOT NULL,
  id_attachment int NOT NULL,
  PRIMARY KEY (id_product)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Describe the impact on weight / price of an attribute */
CREATE TABLE PREFIX_attribute_impact (
  id_attribute_impact int NOT NULL,
  id_product int NOT NULL,
  id_attribute int NOT NULL,
  weight int NOT NULL,
  price int NOT NULL,
  PRIMARY KEY (id_attribute_impact)
  -- UNIQUE KEY id_product (id_product, id_attribute)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Describe the carrier informations */
CREATE TABLE PREFIX_carrier (
  id_carrier int NOT NULL,
  id_reference int NOT NULL,
  id_tax_rules_group int,
  name text NOT NULL,
  url text,
  active int NOT NULL,
  deleted int NOT NULL,
  shipping_handling int NOT NULL,
  range_behavior int NOT NULL,
  is_module int NOT NULL,
  is_free int NOT NULL,
  shipping_external int NOT NULL,
  need_range int NOT NULL,
  external_module_name text,
  shipping_method int NOT NULL,
  position int NOT NULL,
  max_width int,
  max_height int,
  max_depth int,
  max_weight int,
  grade int,
  PRIMARY KEY (id_carrier)
  -- KEY deleted (deleted, active),
  -- KEY id_tax_rules_group (id_tax_rules_group),
  -- KEY reference (
  --   id_reference, deleted, active
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Localization carrier infos */
CREATE TABLE PREFIX_carrier_lang (
  id_carrier int NOT NULL,
  id_shop int NOT NULL,
  id_lang int NOT NULL,
  delay text,
  PRIMARY KEY (id_carrier)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Association between a zone and a carrier */
CREATE TABLE PREFIX_carrier_zone (
  id_carrier int NOT NULL,
  id_zone int NOT NULL,
  PRIMARY KEY (id_carrier)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Describe the metadata associated with the carts */
CREATE TABLE PREFIX_cart (
  id_cart int NOT NULL,
  id_shop_group int NOT NULL,
  id_shop int NOT NULL,
  id_carrier int NOT NULL,
  delivery_option text NOT NULL,
  id_lang int NOT NULL,
  id_address_delivery int NOT NULL,
  id_address_invoice int NOT NULL,
  id_currency int NOT NULL,
  id_customer int NOT NULL,
  id_guest int NOT NULL,
  secure_key text NOT NULL,
  recyclable int NOT NULL,
  gift int NOT NULL,
  gift_message text,
  mobile_theme int NOT NULL,
  allow_seperated_package int NOT NULL,
  date_add datetime NOT NULL,
  date_upd datetime NOT NULL,
  checkout_session_data text NOT NULL,
  PRIMARY KEY (id_cart)
  -- KEY cart_customer (id_customer),
  -- KEY id_address_delivery (id_address_delivery),
  -- KEY id_address_invoice (id_address_invoice),
  -- KEY id_carrier (id_carrier),
  -- KEY id_lang (id_lang),
  -- KEY id_currency (id_currency),
  -- KEY id_guest (id_guest),
  -- KEY id_shop_group (id_shop_group),
  -- KEY id_shop_2 (id_shop, date_upd),
  -- KEY id_shop (id_shop, date_add)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Contains all the promo code rules */
CREATE TABLE PREFIX_cart_rule (
  id_cart_rule int NOT NULL,
  id_customer int NOT NULL,
  date_from datetime NOT NULL,
  date_to datetime NOT NULL,
  description text,
  quantity int NOT NULL,
  quantity_per_user int NOT NULL,
  priority int NOT NULL,
  partial_use int NOT NULL,
  code text NOT NULL,
  minimum_amount int NOT NULL,
  minimum_amount_tax int NOT NULL,
  minimum_amount_currency int NOT NULL,
  minimum_amount_shipping int NOT NULL,
  country_restriction int NOT NULL,
  carrier_restriction int NOT NULL,
  group_restriction int NOT NULL,
  cart_rule_restriction int NOT NULL,
  product_restriction int NOT NULL,
  shop_restriction int NOT NULL,
  free_shipping int NOT NULL,
  reduction_percent int NOT NULL,
  reduction_amount int NOT NULL,
  reduction_tax int NOT NULL,
  reduction_currency int NOT NULL,
  reduction_product int NOT NULL,
  reduction_exclude_special int NOT NULL,
  gift_product int NOT NULL,
  gift_product_attribute int NOT NULL,
  highlight int NOT NULL,
  active int NOT NULL,
  date_add datetime NOT NULL,
  date_upd datetime NOT NULL,
  PRIMARY KEY (id_cart_rule)
  -- KEY id_customer (
  --   id_customer, active, date_to
  -- ),
  -- KEY group_restriction (
  --   group_restriction, active, date_to
  -- ),
  -- KEY id_customer_2 (
  --   id_customer, active, highlight,
  --   date_to
  -- ),
  -- KEY group_restriction_2 (
  --   group_restriction, active, highlight,
  --   date_to
  -- ),
  -- KEY date_from (date_from),
  -- KEY date_to (date_to)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Localized name assocatied with a promo code */
CREATE TABLE PREFIX_cart_rule_lang (
  id_cart_rule int NOT NULL,
  id_lang int NOT NULL,
  name text NOT NULL,
  PRIMARY KEY (id_cart_rule)
  -- PRIMARY KEY (id_cart_rule, id_lang)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Country associated with a promo code */
CREATE TABLE PREFIX_cart_rule_country (
  id_cart_rule int NOT NULL,
  id_country int NOT NULL,
  PRIMARY KEY (id_cart_rule)
  -- PRIMARY KEY (id_cart_rule, id_country)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* User group associated with a promo code */
CREATE TABLE PREFIX_cart_rule_group (
  id_cart_rule int NOT NULL,
  id_group int NOT NULL,
  PRIMARY KEY (id_cart_rule)
  -- PRIMARY KEY (id_cart_rule, id_group)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Carrier associated with a promo code */
CREATE TABLE PREFIX_cart_rule_carrier (
  id_cart_rule int NOT NULL,
  id_carrier int NOT NULL,
  PRIMARY KEY (id_cart_rule)
  -- PRIMARY KEY (id_cart_rule, id_carrier)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Allowed combination of promo code */
CREATE TABLE PREFIX_cart_rule_combination (
  id_cart_rule_1 int NOT NULL,
  id_cart_rule_2 int NOT NULL,
  PRIMARY KEY (id_cart_rule_1)
  -- PRIMARY KEY (
  --   id_cart_rule_1, id_cart_rule_2
  -- ),
  -- KEY id_cart_rule_1 (id_cart_rule_1),
  -- KEY id_cart_rule_2 (id_cart_rule_2)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* @TODO : check checkProductRestrictionsFromCart() to understand the code */
CREATE TABLE PREFIX_cart_rule_product_rule_group (
  id_product_rule_group int NOT NULL,
  id_cart_rule int NOT NULL,
  quantity int NOT NULL,
  PRIMARY KEY (id_product_rule_group)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* @TODO : check checkProductRestrictionsFromCart() to understand the code */
CREATE TABLE PREFIX_cart_rule_product_rule (
  id_product_rule int NOT NULL,
  id_product_rule_group int NOT NULL,
  type text NOT NULL,
  PRIMARY KEY (id_product_rule)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* @TODO : check checkProductRestrictionsFromCart() to understand the code */
CREATE TABLE PREFIX_cart_rule_product_rule_value (
  id_product_rule int NOT NULL,
  id_item int NOT NULL,
  PRIMARY KEY (id_product_rule)
  -- PRIMARY KEY (id_product_rule, id_item)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Association between a cart and a promo code */
CREATE TABLE PREFIX_cart_cart_rule (
  id_cart int NOT NULL,
  id_cart_rule int NOT NULL,
  PRIMARY KEY (id_cart)
  -- PRIMARY KEY (id_cart, id_cart_rule),
  -- KEY (id_cart_rule)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Association between a shop and a promo code */
CREATE TABLE PREFIX_cart_rule_shop (
  id_cart_rule int NOT NULL,
  id_shop int NOT NULL,
  PRIMARY KEY (id_cart_rule)
  -- PRIMARY KEY (id_cart_rule, id_shop)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* List of products inside a cart */
CREATE TABLE PREFIX_cart_product (
  id_cart int NOT NULL,
  id_product int NOT NULL,
  id_address_delivery int NOT NULL,
  id_shop int NOT NULL,
  id_product_attribute int NOT NULL,
  id_customization int NOT NULL,
  quantity int NOT NULL,
  date_add datetime NOT NULL,
  PRIMARY KEY (id_cart)
  -- PRIMARY KEY (
  --   id_cart, id_product, id_product_attribute,
  --   id_customization, id_address_delivery
  -- ),
  -- KEY id_product_attribute (id_product_attribute),
  -- KEY id_cart_order (
  --   id_cart, date_add, id_product,
  --   id_product_attribute
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* List of product categories */
CREATE TABLE PREFIX_category (
  id_category int NOT NULL,
  id_parent int NOT NULL,
  id_shop_default int NOT NULL,
  level_depth int NOT NULL,
  nleft int NOT NULL,
  nright int NOT NULL,
  active int NOT NULL,
  date_add datetime NOT NULL,
  date_upd datetime NOT NULL,
  position int NOT NULL,
  is_root_category int NOT NULL,
  PRIMARY KEY (id_category)
  -- KEY category_parent (id_parent),
  -- KEY nleftrightactive (nleft, nright, active),
  -- KEY level_depth (level_depth),
  -- KEY nright (nright),
  -- KEY activenleft (active, nleft),
  -- KEY activenright (active, nright)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Association between a product category and a group of customer */
CREATE TABLE PREFIX_category_group (
  id_category int NOT NULL,
  id_group int NOT NULL,
  PRIMARY KEY (id_category)
  -- PRIMARY KEY (id_category, id_group),
  -- KEY id_category (id_category),
  -- KEY id_group (id_group)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Localized product category infos */
CREATE TABLE PREFIX_category_lang (
  id_category int NOT NULL,
  id_shop int NOT NULL,
  id_lang int NOT NULL,
  name text NOT NULL,
  description text,
  link_rewrite text NOT NULL,
  meta_title text,
  meta_keywords text,
  meta_description text,
  PRIMARY KEY (
    id_category
  )
  -- PRIMARY KEY (
  --   id_category, id_shop, id_lang
  -- ),
  -- KEY category_name (name)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Association between a product category and a product */
CREATE TABLE PREFIX_category_product (
  id_category int NOT NULL,
  id_product int NOT NULL,
  position int NOT NULL,
  PRIMARY KEY (id_category)
  -- PRIMARY KEY (id_category, id_product),
  -- INDEX (id_product),
  -- INDEX (id_category, position)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Information on content block position and category */
CREATE TABLE PREFIX_cms (
  id_cms int NOT NULL,
  id_cms_category int NOT NULL,
  position int NOT NULL,
  active int NOT NULL,
  indexation int NOT NULL,
  PRIMARY KEY (id_cms)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Localized CMS infos */
CREATE TABLE PREFIX_cms_lang (
  id_cms int NOT NULL,
  id_lang int NOT NULL,
  id_shop int NOT NULL,
  meta_title text NOT NULL,
  head_seo_title text,
  meta_description text,
  meta_keywords text,
  content text,
  link_rewrite text NOT NULL,
  PRIMARY KEY (id_cms)
  -- PRIMARY KEY (id_cms, id_shop, id_lang)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* CMS category informations */
CREATE TABLE PREFIX_cms_category (
  id_cms_category int NOT NULL,
  id_parent int NOT NULL,
  level_depth int NOT NULL,
  active int NOT NULL,
  date_add datetime NOT NULL,
  date_upd datetime NOT NULL,
  position int NOT NULL,
  PRIMARY KEY (id_cms_category)
  -- KEY category_parent (id_parent)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Localized CMS category info */
CREATE TABLE PREFIX_cms_category_lang (
  id_cms_category int NOT NULL,
  id_lang int NOT NULL,
  id_shop int NOT NULL,
  name text NOT NULL,
  description text,
  link_rewrite text NOT NULL,
  meta_title text,
  meta_keywords text,
  meta_description text,
  PRIMARY KEY (id_cms_category)
  -- PRIMARY KEY (
  --   id_cms_category, id_shop, id_lang
  -- ),
  -- KEY category_name (name)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Association between a CMS category and a shop */
CREATE TABLE PREFIX_cms_category_shop (
  id_cms_category int NOT NULL,
  id_shop int NOT NULL,
  PRIMARY KEY (id_cms_category)
  -- PRIMARY KEY (id_cms_category, id_shop),
  -- KEY id_shop (id_shop)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Store the configuration, depending on the shop & the group. See configuration.xml to have the list of
existing variables */
CREATE TABLE PREFIX_configuration (
  id_configuration int NOT NULL,
  id_shop_group int,
  id_shop int,
  name text NOT NULL,
  value text,
  date_add datetime NOT NULL,
  date_upd datetime NOT NULL,
  PRIMARY KEY (id_configuration)
  -- KEY name (name),
  -- KEY id_shop (id_shop),
  -- KEY id_shop_group (id_shop_group)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Localized configuration info */
CREATE TABLE PREFIX_configuration_lang (
  id_configuration int NOT NULL,
  id_lang int NOT NULL,
  value text,
  date_upd datetime,
  PRIMARY KEY (id_configuration)
  -- PRIMARY KEY (id_configuration, id_lang)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Store the KPI configuration variables (dashboard) */
CREATE TABLE PREFIX_configuration_kpi (
  id_configuration_kpi int NOT NULL,
  id_shop_group int,
  id_shop int,
  name text NOT NULL,
  value text,
  date_add datetime NOT NULL,
  date_upd datetime NOT NULL,
  PRIMARY KEY (id_configuration_kpi)
  -- KEY name (name),
  -- KEY id_shop (id_shop),
  -- KEY id_shop_group (id_shop_group)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Localized KPI configuration label */
CREATE TABLE PREFIX_configuration_kpi_lang (
  id_configuration_kpi int NOT NULL,
  id_lang int NOT NULL,
  value text,
  date_upd datetime,
  PRIMARY KEY (id_configuration_kpi)
  -- PRIMARY KEY (
  --   id_configuration_kpi, id_lang
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* User connections log. See PS_STATSDATA_PAGESVIEWS variable */
CREATE TABLE PREFIX_connections (
  id_connections int NOT NULL,
  id_shop_group int NOT NULL,
  id_shop int NOT NULL,
  id_guest int NOT NULL,
  id_page int NOT NULL,
  ip_address int NOT NULL,
  date_add datetime NOT NULL,
  http_referer text,
  PRIMARY KEY (id_connections)
  -- KEY id_guest (id_guest),
  -- KEY date_add (date_add),
  -- KEY id_page (id_page)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* User connection pages log. See PS_STATSDATA_CUSTOMER_PAGESVIEWS variable */
CREATE TABLE PREFIX_connections_page (
  id_connections int NOT NULL,
  id_page int NOT NULL,
  time_start datetime NOT NULL,
  time_end datetime,
  PRIMARY KEY (id_connections)
  -- PRIMARY KEY (
  --   id_connections, id_page, time_start
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* User connection source log. */
CREATE TABLE PREFIX_connections_source (
  id_connections_source int NOT NULL,
  id_connections int NOT NULL,
  http_referer text,
  request_uri text,
  keywords text,
  date_add datetime NOT NULL,
  PRIMARY KEY (id_connections_source)
  -- KEY connections (id_connections),
  -- KEY orderby (date_add),
  -- KEY http_referer (http_referer),
  -- KEY request_uri (request_uri)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Store technical contact informations */
CREATE DATA_SUBJECT TABLE PREFIX_contact (
  id_contact int NOT NULL,
  email text NOT NULL,
  customer_service int NOT NULL,
  position int NOT NULL,
  PRIMARY KEY (id_contact)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Localized technical contact infos */
CREATE TABLE PREFIX_contact_lang (
  id_contact int NOT NULL,
  id_lang int NOT NULL,
  name text NOT NULL,
  description text,
  PRIMARY KEY (id_contact)
  -- PRIMARY KEY (id_contact, id_lang)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Country specific data */
CREATE TABLE PREFIX_country (
  id_country int NOT NULL,
  id_zone int NOT NULL,
  id_currency int NOT NULL,
  iso_code text NOT NULL,
  call_prefix int NOT NULL,
  active int NOT NULL,
  contains_states int NOT NULL,
  need_identification_number int NOT NULL,
  need_zip_code int NOT NULL,
  zip_code_format text NOT NULL,
  display_tax_label int NOT NULL,
  PRIMARY KEY (id_country)
  -- KEY country_iso_code (iso_code),
  -- KEY country_ (id_zone)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Localized country information */
CREATE TABLE PREFIX_country_lang (
  id_country int NOT NULL,
  id_lang int NOT NULL,
  name text NOT NULL,
  PRIMARY KEY (id_country)
  -- PRIMARY KEY (id_country, id_lang)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Currency specification */
CREATE TABLE PREFIX_currency (
  id_currency int NOT NULL,
  name text NOT NULL, /* Deprecated since 1.7.5.0. Use PREFIX_currency_lang.name instead. */
  iso_code text NOT NULL,
  numeric_iso_code text,
  precision int NOT NULL,
  conversion_rate int NOT NULL,
  deleted int NOT NULL,
  active int NOT NULL,
  unofficial int NOT NULL,
  modified int NOT NULL,
  PRIMARY KEY (id_currency)
  -- KEY currency_iso_code (iso_code)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Localized currency information */
CREATE TABLE PREFIX_currency_lang (
  id_currency int NOT NULL,
  id_lang int NOT NULL,
  name text NOT NULL,
  symbol text NOT NULL,
  pattern text,
  PRIMARY KEY (id_currency)
  -- PRIMARY KEY (id_currency,id_lang)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Customer info */
CREATE DATA_SUBJECT TABLE PREFIX_customer (
  id_customer int NOT NULL,
  id_shop_group int NOT NULL,
  id_shop int NOT NULL,
  id_gender int NOT NULL,
  id_default_group int NOT NULL,
  id_lang int NOT NULL,
  id_risk int NOT NULL,
  company text,
  siret text,
  ape text,
  firstname text NOT NULL,
  lastname text NOT NULL,
  email text NOT NULL,
  passwd text NOT NULL,
  last_passwd_gen datetime NOT NULL,
  birthday datetime,
  newsletter int NOT NULL,
  ip_registration_newsletter text,
  newsletter_date_add datetime,
  optin int NOT NULL,
  website text,
  outstanding_allow_amount int NOT NULL,
  show_public_prices int NOT NULL,
  max_payment_days int NOT NULL,
  secure_key text NOT NULL,
  note text,
  active int NOT NULL,
  is_guest int NOT NULL,
  deleted int NOT NULL,
  date_add datetime NOT NULL,
  date_upd datetime NOT NULL,
  reset_password_token text,
  reset_password_validity datetime,
  PRIMARY KEY (id_customer)
  -- KEY customer_email (email),
  -- KEY customer_login (email, passwd),
  -- KEY id_customer_passwd (id_customer, passwd),
  -- KEY id_gender (id_gender),
  -- KEY id_shop_group (id_shop_group),
  -- KEY id_shop (id_shop, date_add)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Customer group association */
CREATE TABLE PREFIX_customer_group (
  id_customer int NOT NULL,
  id_group int NOT NULL,
  PRIMARY KEY (id_customer)
  -- PRIMARY KEY (id_customer, id_group),
  -- INDEX customer_login(id_group),
  -- KEY id_customer (id_customer)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Customer support private messaging */
CREATE TABLE PREFIX_customer_message (
  id_customer_message int NOT NULL,
  id_customer_thread int,
  id_employee int,
  message text NOT NULL,
  file_name text,
  ip_address text,
  user_agent text,
  date_add datetime NOT NULL,
  date_upd datetime NOT NULL,
  private int NOT NULL,
  read int NOT NULL,
  PRIMARY KEY (id_customer_message)
  -- KEY id_customer_thread (id_customer_thread),
  -- KEY id_employee (id_employee)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* store the header of already fetched emails from imap support messaging */
CREATE TABLE PREFIX_customer_message_sync_imap (
  md5_header text NOT NULL,
  -- KEY md5_header_index (
  --   md5_header(4)
  -- )
  PRIMARY KEY (md5_header)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Customer support private messaging */
CREATE TABLE PREFIX_customer_thread (
  id_customer_thread int NOT NULL,
  id_shop int NOT NULL,
  id_lang int NOT NULL,
  id_contact int NOT NULL,
  id_customer int,
  id_order int,
  id_product int,
  status text NOT NULL,
  email text NOT NULL,
  token text,
  date_add datetime NOT NULL,
  date_upd datetime NOT NULL,
  PRIMARY KEY (id_customer_thread)
  -- KEY id_shop (id_shop),
  -- KEY id_lang (id_lang),
  -- KEY id_contact (id_contact),
  -- KEY id_customer (id_customer),
  -- KEY id_order (id_order),
  -- KEY id_product (id_product)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Customization associated with a purchase (engraving...) */
CREATE TABLE PREFIX_customization (
  id_customization int NOT NULL,
  id_product_attribute int NOT NULL,
  id_address_delivery int NOT NULL,
  id_cart int NOT NULL,
  id_product int NOT NULL,
  quantity int NOT NULL,
  quantity_refunded int NOT NULL,
  quantity_returned int NOT NULL,
  in_cart int NOT NULL,
  PRIMARY KEY (id_customization)
  -- PRIMARY KEY (
  --   id_customization, id_cart, id_product,
  --   id_address_delivery
  -- ),
  -- KEY id_product_attribute (id_product_attribute),
  -- KEY id_cart_product (
  --   id_cart, id_product, id_product_attribute
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Customization possibility for a product */
CREATE TABLE PREFIX_customization_field (
  id_customization_field int NOT NULL,
  id_product int NOT NULL,
  type int NOT NULL,
  required int NOT NULL,
  is_module int NOT NULL,
  is_deleted int NOT NULL,
  PRIMARY KEY (id_customization_field)
  -- KEY id_product (id_product)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Localized customization fields */
CREATE TABLE PREFIX_customization_field_lang (
  id_customization_field int NOT NULL,
  id_lang int NOT NULL,
  id_shop int NOT NULL,
  name text NOT NULL,
  PRIMARY KEY (id_customization_field)
  -- PRIMARY KEY (
  --   id_customization_field, id_lang,
  --   id_shop
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Customization content associated with a purchase (e.g. : text to engrave) */
CREATE TABLE PREFIX_customized_data (
  id_customization int NOT NULL,
  type int NOT NULL,
  -- index int NOT NULL,
  value text NOT NULL,
  id_module int NOT NULL,
  price int NOT NULL,
  weight int NOT NULL,
  PRIMARY KEY (id_customization)
  -- PRIMARY KEY (
  --   id_customization, type, index
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Date range info (used in PS_STATSDATA_PAGESVIEWS mode) */
CREATE TABLE PREFIX_date_range (
  id_date_range int NOT NULL,
  time_start datetime NOT NULL,
  time_end datetime NOT NULL,
  PRIMARY KEY (id_date_range)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Delivery info associated with a carrier and a shop */
CREATE TABLE PREFIX_delivery (
  id_delivery int NOT NULL,
  id_shop int NOT NULL,
  id_shop_group int NOT NULL,
  id_carrier int NOT NULL,
  id_range_price int,
  id_range_weight int,
  id_zone int NOT NULL,
  price int NOT NULL,
  PRIMARY KEY (id_delivery)
  -- KEY id_zone (id_zone),
  -- KEY id_carrier (id_carrier, id_zone),
  -- KEY id_range_price (id_range_price),
  -- KEY id_range_weight (id_range_weight)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Admin users */
CREATE DATA_SUBJECT TABLE PREFIX_employee (
  id_employee int NOT NULL,
  id_profile int NOT NULL,
  id_lang int NOT NULL,
  lastname text NOT NULL,
  firstname text NOT NULL,
  email text NOT NULL,
  passwd text NOT NULL,
  last_passwd_gen datetime NOT NULL,
  stats_date_from datetime,
  stats_date_to datetime,
  stats_compare_from datetime,
  stats_compare_to datetime,
  stats_compare_option int NOT NULL,
  preselect_date_range text,
  bo_color text,
  bo_theme text,
  bo_css text,
  default_tab int NOT NULL,
  bo_width int NOT NULL,
  bo_menu int NOT NULL,
  active int NOT NULL,
  optin int,
  id_last_order int NOT NULL,
  id_last_customer_message int NOT NULL,
  id_last_customer int NOT NULL,
  last_connection_date datetime,
  reset_password_token text,
  reset_password_validity datetime,
  PRIMARY KEY (id_employee)
  -- KEY employee_login (email, passwd),
  -- KEY id_employee_passwd (id_employee, passwd),
  -- KEY id_profile (id_profile)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Admin users shop */
CREATE TABLE PREFIX_employee_shop (
  id_employee int NOT NULL,
  id_shop int NOT NULL,
  PRIMARY KEY (id_employee)
  -- KEY id_shop (id_shop)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Position of each feature */
CREATE TABLE PREFIX_feature (
  id_feature int NOT NULL,
  position int NOT NULL,
  PRIMARY KEY (id_feature)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Localized feature info */
CREATE TABLE PREFIX_feature_lang (
  id_feature int NOT NULL,
  id_lang int NOT NULL,
  name text,
  PRIMARY KEY (id_feature)
  -- KEY (id_lang, name)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Association between a feature and a product */
CREATE TABLE PREFIX_feature_product (
  id_feature int NOT NULL,
  id_product int NOT NULL,
  id_feature_value int NOT NULL,
  PRIMARY KEY (id_feature)
  -- KEY id_feature_value (id_feature_value),
  -- KEY id_product (id_product)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Various choice associated with a feature */
CREATE TABLE PREFIX_feature_value (
  id_feature_value int NOT NULL,
  id_feature int NOT NULL,
  custom int,
  PRIMARY KEY (id_feature_value)
  -- KEY feature (id_feature)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Localized feature choice */
CREATE TABLE PREFIX_feature_value_lang (
  id_feature_value int NOT NULL,
  id_lang int NOT NULL,
  value text,
  PRIMARY KEY (id_feature_value)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* User titles (e.g. : Mr, Mrs...) */
CREATE TABLE PREFIX_gender (
  id_gender int NOT NULL,
  type int NOT NULL,
  PRIMARY KEY (id_gender)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Localized user title */
CREATE TABLE PREFIX_gender_lang (
  id_gender int NOT NULL,
  id_lang int NOT NULL,
  name text NOT NULL,
  PRIMARY KEY (id_gender)
  -- KEY id_gender (id_gender)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Group special price rules */
CREATE TABLE PREFIX_group (
  id_group int NOT NULL,
  reduction int NOT NULL,
  price_display_method int NOT NULL,
  show_prices int NOT NULL,
  date_add datetime NOT NULL,
  date_upd datetime NOT NULL,
  PRIMARY KEY (id_group)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Localized group info */
CREATE TABLE PREFIX_group_lang (
  id_group int NOT NULL,
  id_lang int NOT NULL,
  name text NOT NULL,
  PRIMARY KEY (id_group)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Category specific reduction */
CREATE TABLE PREFIX_group_reduction (
  id_group_reduction int NOT NULL,
  id_group int NOT NULL,
  id_category int NOT NULL,
  reduction int NOT NULL,
  PRIMARY KEY (id_group_reduction)
  -- UNIQUE KEY(id_group, id_category)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Cache which store product price after reduction */
CREATE TABLE PREFIX_product_group_reduction_cache (
  id_product int NOT NULL,
  id_group int NOT NULL,
  reduction int NOT NULL,
  PRIMARY KEY (id_product)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Specify a carrier for a given product */
CREATE TABLE PREFIX_product_carrier (
  id_product int NOT NULL,
  id_carrier_reference int NOT NULL,
  id_shop int NOT NULL,
  PRIMARY KEY (id_product)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Stats from guest user */
CREATE TABLE PREFIX_guest (
  id_guest int NOT NULL,
  id_operating_system int,
  id_web_browser int,
  id_customer int,
  javascript int,
  screen_resolution_x int,
  screen_resolution_y int,
  screen_color int,
  sun_java int,
  adobe_flash int,
  adobe_director int,
  apple_quicktime int,
  real_player int,
  windows_media int,
  accept_language text,
  mobile_theme int NOT NULL,
  PRIMARY KEY (id_guest)
  -- KEY id_customer (id_customer),
  -- KEY id_operating_system (id_operating_system),
  -- KEY id_web_browser (id_web_browser)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Store hook description */
CREATE TABLE PREFIX_hook (
  id_hook int NOT NULL,
  name text NOT NULL,
  title text NOT NULL,
  description text,
  position int NOT NULL,
  PRIMARY KEY (id_hook)
  -- UNIQUE KEY hook_name (name)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Hook alias name */
CREATE TABLE PREFIX_hook_alias (
  id_hook_alias int NOT NULL,
  alias text NOT NULL,
  name text NOT NULL,
  PRIMARY KEY (id_hook_alias)
  -- UNIQUE KEY alias (alias)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Define registered hook module */
CREATE TABLE PREFIX_hook_module (
  id_module int NOT NULL,
  id_shop int NOT NULL,
  id_hook int NOT NULL,
  position int NOT NULL,
  PRIMARY KEY (id_module)
  -- PRIMARY KEY (
  --   id_module, id_hook, id_shop
  -- ),
  -- KEY id_hook (id_hook),
  -- KEY id_module (id_module),
  -- KEY position (id_shop, position)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* List of page type where the hook is not loaded */
CREATE TABLE PREFIX_hook_module_exceptions (
  id_hook_module_exceptions int NOT NULL,
  id_shop int NOT NULL,
  id_module int NOT NULL,
  id_hook int NOT NULL,
  file_name text,
  PRIMARY KEY (id_hook_module_exceptions)
  -- KEY id_module (id_module),
  -- KEY id_hook (id_hook)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Product image info */
CREATE TABLE PREFIX_image (
  id_image int NOT NULL,
  id_product int NOT NULL,
  position int NOT NULL,
  cover int NOT NULL,
  PRIMARY KEY (id_image)
  -- KEY image_product (id_product),
  -- UNIQUE KEY id_product_cover (id_product, cover),
  -- UNIQUE KEY idx_product_image (
  --   id_image, id_product, cover
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Localized product image */
CREATE TABLE PREFIX_image_lang (
  id_image int NOT NULL,
  id_lang int NOT NULL,
  legend text,
  PRIMARY KEY (id_image)
  -- PRIMARY KEY (id_image, id_lang)
  -- KEY id_image (id_image)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Image type description */
CREATE TABLE PREFIX_image_type (
  id_image_type int NOT NULL,
  name text NOT NULL,
  width int NOT NULL,
  height int NOT NULL,
  products int NOT NULL,
  categories int NOT NULL,
  manufacturers int NOT NULL,
  suppliers int NOT NULL,
  stores int NOT NULL,
  PRIMARY KEY (id_image_type)
  -- KEY image_type_name (name)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Manufacturer info */
CREATE TABLE PREFIX_manufacturer (
  id_manufacturer int NOT NULL,
  name text NOT NULL,
  date_add datetime NOT NULL,
  date_upd datetime NOT NULL,
  active int NOT NULL,
  PRIMARY KEY (id_manufacturer)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* localized manufacturer info */
CREATE TABLE PREFIX_manufacturer_lang (
  id_manufacturer int NOT NULL,
  id_lang int NOT NULL,
  description text,
  short_description text,
  meta_title text,
  meta_keywords text,
  meta_description text,
  PRIMARY KEY (id_manufacturer)
  -- PRIMARY KEY (id_manufacturer, id_lang)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Private messaging */
CREATE TABLE PREFIX_message (
  id_message int NOT NULL,
  id_cart int,
  id_customer int NOT NULL,
  id_employee int,
  id_order int NOT NULL,
  message text NOT NULL,
  private int NOT NULL,
  date_add datetime NOT NULL,
  PRIMARY KEY (id_message)
  -- KEY message_order (id_order),
  -- KEY id_cart (id_cart),
  -- KEY id_customer (id_customer),
  -- KEY id_employee (id_employee)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Private messaging read flag */
CREATE TABLE PREFIX_message_readed (
  id_message int NOT NULL,
  id_employee int NOT NULL,
  date_add datetime NOT NULL,
  PRIMARY KEY (id_message)
  -- PRIMARY KEY (id_message, id_employee)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* List of route type that can be localized */
CREATE TABLE PREFIX_meta (
  id_meta int NOT NULL,
  page text NOT NULL,
  configurable int NOT NULL,
  PRIMARY KEY (id_meta)
  -- UNIQUE KEY page (page)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Localized routes */
CREATE TABLE PREFIX_meta_lang (
  id_meta int NOT NULL,
  id_shop int NOT NULL,
  id_lang int NOT NULL,
  title text,
  description text,
  keywords text,
  url_rewrite text NOT NULL,
  PRIMARY KEY (id_meta)
  -- PRIMARY KEY (id_meta, id_shop, id_lang),
  -- KEY id_shop (id_shop),
  -- KEY id_lang (id_lang)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Installed module list */
CREATE TABLE PREFIX_module (
  id_module int NOT NULL,
  name text NOT NULL,
  active int NOT NULL,
  version text NOT NULL,
  PRIMARY KEY (id_module)
  -- UNIQUE KEY name_UNIQUE (name),
  -- KEY name (name)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Module / class authorization_role */
CREATE TABLE PREFIX_authorization_role (
  id_authorization_role int NOT NULL,
  slug text NOT NULL,
  PRIMARY KEY (id_authorization_role)
  -- UNIQUE KEY (slug)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Association between a profile and a tab authorization_role (can be 'CREATE', 'READ', 'UPDATE' or 'DELETE') */
CREATE TABLE PREFIX_access (
  id_profile int NOT NULL,
  id_authorization_role int NOT NULL,
  PRIMARY KEY (id_profile)
  -- PRIMARY KEY (
  --   id_profile, id_authorization_role
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Association between a profile and a module authorization_role (can be 'CREATE', 'READ', 'UPDATE' or 'DELETE') */
CREATE TABLE PREFIX_module_access (
  id_profile int NOT NULL,
  id_authorization_role int NOT NULL,
  PRIMARY KEY (id_profile)
  -- PRIMARY KEY (
  --   id_profile, id_authorization_role
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* countries allowed for each module (e.g. : countries supported for a payment module) */
CREATE TABLE PREFIX_module_country (
  id_module int NOT NULL,
  id_shop int NOT NULL,
  id_country int NOT NULL,
  PRIMARY KEY (id_module)
  -- PRIMARY KEY (
  --   id_module, id_shop, id_country
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* currencies allowed for each module */
CREATE TABLE PREFIX_module_currency (
  id_module int NOT NULL,
  id_shop int NOT NULL,
  id_currency int NOT NULL,
  PRIMARY KEY (id_module)
  -- PRIMARY KEY (
  --   id_module, id_shop, id_currency
  -- ),
  -- KEY id_module (id_module)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* groups allowed for each module */
CREATE TABLE PREFIX_module_group (
  id_module int NOT NULL,
  id_shop int NOT NULL,
  id_group int NOT NULL,
  PRIMARY KEY (id_module)
  -- PRIMARY KEY (
  --   id_module, id_shop, id_group
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* carriers allowed for each module */
CREATE TABLE PREFIX_module_carrier (
  id_module int NOT NULL,
  id_shop int NOT NULL,
  id_reference int NOT NULL,
  PRIMARY KEY (id_module)
  -- PRIMARY KEY (
  --   id_module, id_shop, id_reference
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* List of OS (used in guest stats) */
CREATE TABLE PREFIX_operating_system (
  id_operating_system int NOT NULL,
  name text,
  PRIMARY KEY (id_operating_system)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* List of orders */
CREATE TABLE PREFIX_orders (
  id_order int NOT NULL,
  reference text,
  id_shop_group int NOT NULL,
  id_shop int NOT NULL,
  id_carrier int NOT NULL,
  id_lang int NOT NULL,
  id_customer int NOT NULL,
  id_cart int NOT NULL,
  id_currency int NOT NULL,
  id_address_delivery int NOT NULL,
  id_address_invoice int NOT NULL,
  current_state int NOT NULL,
  secure_key text NOT NULL,
  payment text NOT NULL,
  conversion_rate int NOT NULL,
  module text,
  recyclable int NOT NULL,
  gift int NOT NULL,
  gift_message text,
  mobile_theme int NOT NULL,
  shipping_number text,
  total_discounts int NOT NULL,
  total_discounts_tax_incl int NOT NULL,
  total_discounts_tax_excl int NOT NULL,
  total_paid int NOT NULL,
  total_paid_tax_incl int NOT NULL,
  total_paid_tax_excl int NOT NULL,
  total_paid_real int NOT NULL,
  total_products int NOT NULL,
  total_products_wt int NOT NULL,
  total_shipping int NOT NULL,
  total_shipping_tax_incl int NOT NULL,
  total_shipping_tax_excl int NOT NULL,
  carrier_tax_rate int NOT NULL,
  total_wrapping int NOT NULL,
  total_wrapping_tax_incl int NOT NULL,
  total_wrapping_tax_excl int NOT NULL,
  round_mode int NOT NULL,
  round_type int NOT NULL,
  invoice_number int NOT NULL,
  delivery_number int NOT NULL,
  invoice_date datetime NOT NULL,
  delivery_date datetime NOT NULL,
  valid int NOT NULL,
  date_add datetime NOT NULL,
  date_upd datetime NOT NULL,
  PRIMARY KEY (id_order)
  -- KEY reference (reference),
  -- KEY id_customer (id_customer),
  -- KEY id_cart (id_cart),
  -- KEY invoice_number (invoice_number),
  -- KEY id_carrier (id_carrier),
  -- KEY id_lang (id_lang),
  -- KEY id_currency (id_currency),
  -- KEY id_address_delivery (id_address_delivery),
  -- KEY id_address_invoice (id_address_invoice),
  -- KEY id_shop_group (id_shop_group),
  -- KEY (current_state),
  -- KEY id_shop (id_shop),
  -- INDEX date_add(date_add)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Order tax detail */
CREATE TABLE PREFIX_order_detail_tax (
  id_order_detail int NOT NULL,
  id_tax int NOT NULL,
  unit_amount int NOT NULL,
  total_amount int NOT NULL,
  PRIMARY KEY (id_order_detail)
  -- KEY (id_order_detail),
  -- KEY id_tax (id_tax)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* list of invoice */
CREATE TABLE PREFIX_order_invoice (
  id_order_invoice int NOT NULL,
  id_order int NOT NULL,
  number int NOT NULL,
  delivery_number int NOT NULL,
  delivery_date datetime,
  total_discount_tax_excl int NOT NULL,
  total_discount_tax_incl int NOT NULL,
  total_paid_tax_excl int NOT NULL,
  total_paid_tax_incl int NOT NULL,
  total_products int NOT NULL,
  total_products_wt int NOT NULL,
  total_shipping_tax_excl int NOT NULL,
  total_shipping_tax_incl int NOT NULL,
  shipping_tax_computation_method int NOT NULL,
  total_wrapping_tax_excl int NOT NULL,
  total_wrapping_tax_incl int NOT NULL,
  shop_address text,
  note text,
  date_add datetime NOT NULL,
  PRIMARY KEY (id_order_invoice)
  -- KEY id_order (id_order)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* global invoice tax */
CREATE TABLE PREFIX_order_invoice_tax (
  id_order_invoice int NOT NULL,
  type text NOT NULL,
  id_tax int NOT NULL,
  amount int NOT NULL,
  PRIMARY KEY (id_order_invoice)
  -- KEY id_tax (id_tax)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* order detail (every product inside an order) */
CREATE TABLE PREFIX_order_detail (
  id_order_detail int NOT NULL,
  id_order int NOT NULL,
  id_order_invoice int,
  id_warehouse int,
  id_shop int NOT NULL,
  product_id int NOT NULL,
  product_attribute_id int,
  id_customization int,
  product_name text NOT NULL,
  product_quantity int NOT NULL,
  product_quantity_in_stock int NOT NULL,
  product_quantity_refunded int NOT NULL,
  product_quantity_return int NOT NULL,
  product_quantity_reinjected int NOT NULL,
  product_price int NOT NULL,
  reduction_percent int NOT NULL,
  reduction_amount int NOT NULL,
  reduction_amount_tax_incl int NOT NULL,
  reduction_amount_tax_excl int NOT NULL,
  group_reduction int NOT NULL,
  product_quantity_discount int NOT NULL,
  product_ean13 text,
  product_isbn text,
  product_upc text,
  product_mpn text,
  product_reference text,
  product_supplier_reference text,
  product_weight int NOT NULL,
  id_tax_rules_group int,
  tax_computation_method int NOT NULL,
  tax_name text NOT NULL,
  tax_rate int NOT NULL,
  ecotax int NOT NULL,
  ecotax_tax_rate int NOT NULL,
  discount_quantity_applied int NOT NULL,
  download_hash text,
  download_nb int,
  download_deadline datetime,
  total_price_tax_incl int NOT NULL,
  total_price_tax_excl int NOT NULL,
  unit_price_tax_incl int NOT NULL,
  unit_price_tax_excl int NOT NULL,
  total_shipping_price_tax_incl int NOT NULL,
  total_shipping_price_tax_excl int NOT NULL,
  purchase_supplier_price int NOT NULL,
  original_product_price int NOT NULL,
  original_wholesale_price int NOT NULL,
  total_refunded_tax_excl int NOT NULL,
  total_refunded_tax_incl int NOT NULL,
  PRIMARY KEY (id_order_detail)
  -- KEY order_detail_order (id_order),
  -- KEY product_id (
  --   product_id, product_attribute_id
  -- ),
  -- KEY product_attribute_id (product_attribute_id),
  -- KEY id_tax_rules_group (id_tax_rules_group),
  -- KEY id_order_id_order_detail (id_order, id_order_detail)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Promo code used in the order */
CREATE TABLE PREFIX_order_cart_rule (
  id_order_cart_rule int NOT NULL,
  id_order int NOT NULL,
  id_cart_rule int NOT NULL,
  id_order_invoice int,
  name text NOT NULL,
  value int NOT NULL,
  value_tax_excl int NOT NULL,
  free_shipping int NOT NULL,
  PRIMARY KEY (id_order_cart_rule)
  -- KEY id_order (id_order),
  -- KEY id_cart_rule (id_cart_rule)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* order transactional information */
CREATE TABLE PREFIX_order_history (
  id_order_history int NOT NULL,
  id_employee int NOT NULL,
  id_order int NOT NULL,
  id_order_state int NOT NULL,
  date_add datetime NOT NULL,
  PRIMARY KEY (id_order_history)
  -- KEY order_history_order (id_order),
  -- KEY id_employee (id_employee),
  -- KEY id_order_state (id_order_state)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Type of predefined message that can be inserted to an order */
CREATE TABLE PREFIX_order_message (
  id_order_message int NOT NULL,
  date_add datetime NOT NULL,
  PRIMARY KEY (id_order_message)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Localized predefined order message */
CREATE TABLE PREFIX_order_message_lang (
  id_order_message int NOT NULL,
  id_lang int NOT NULL,
  name text NOT NULL,
  message text NOT NULL,
  PRIMARY KEY (id_order_message)
  -- PRIMARY KEY (id_order_message, id_lang)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Return state associated with an order */
CREATE TABLE PREFIX_order_return (
  id_order_return int NOT NULL,
  id_customer int NOT NULL,
  id_order int NOT NULL,
  state int NOT NULL,
  question text NOT NULL,
  date_add datetime NOT NULL,
  date_upd datetime NOT NULL,
  PRIMARY KEY (id_order_return)
  -- KEY order_return_customer (id_customer),
  -- KEY id_order (id_order)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Return detail for each product inside an order */
CREATE TABLE PREFIX_order_return_detail (
  id_order_return int NOT NULL,
  id_order_detail int NOT NULL,
  id_customization int NOT NULL,
  product_quantity int NOT NULL,
  PRIMARY KEY (id_order_return)
  -- PRIMARY KEY (
  --   id_order_return, id_order_detail,
  --   id_customization
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* List of possible return states color */
CREATE TABLE PREFIX_order_return_state (
  id_order_return_state int NOT NULL,
  color text,
  PRIMARY KEY (id_order_return_state)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Localized return states name */
CREATE TABLE PREFIX_order_return_state_lang (
  id_order_return_state int NOT NULL,
  id_lang int NOT NULL,
  name text NOT NULL,
  PRIMARY KEY (id_order_return_state)
  -- PRIMARY KEY (
  --   id_order_return_state, id_lang
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Order slip info */
CREATE TABLE PREFIX_order_slip (
  id_order_slip int NOT NULL,
  conversion_rate int NOT NULL,
  id_customer int NOT NULL,
  id_order int NOT NULL,
  total_products_tax_excl int NOT NULL,
  total_products_tax_incl int NOT NULL,
  total_shipping_tax_excl int NOT NULL,
  total_shipping_tax_incl int NOT NULL,
  shipping_cost int NOT NULL,
  amount int NOT NULL,
  shipping_cost_amount int NOT NULL,
  partial int NOT NULL,
  order_slip_type int NOT NULL,
  date_add datetime NOT NULL,
  date_upd datetime NOT NULL,
  PRIMARY KEY (id_order_slip)
  -- KEY order_slip_customer (id_customer),
  -- KEY id_order (id_order)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Detail of the order slip (every product) */
CREATE TABLE PREFIX_order_slip_detail (
  id_order_slip int NOT NULL,
  id_order_detail int NOT NULL,
  product_quantity int NOT NULL,
  unit_price_tax_excl int NOT NULL,
  unit_price_tax_incl int NOT NULL,
  total_price_tax_excl int NOT NULL,
  total_price_tax_incl int,
  amount_tax_excl int,
  amount_tax_incl int,
  PRIMARY KEY (id_order_slip)
  -- PRIMARY KEY (
  --   id_order_slip, id_order_detail
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* List of available order states */
CREATE TABLE PREFIX_order_state (
  id_order_state int NOT NULL,
  invoice int,
  send_email int NOT NULL,
  module_name text NOT NULL,
  color text,
  unremovable int NOT NULL,
  hidden int NOT NULL,
  logable int NOT NULL,
  delivery int NOT NULL,
  shipped int NOT NULL,
  paid int NOT NULL,
  pdf_invoice int NOT NULL,
  pdf_delivery int NOT NULL,
  deleted int NOT NULL,
  PRIMARY KEY (id_order_state)
  -- KEY module_name (module_name)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Localized order state */
CREATE TABLE PREFIX_order_state_lang (
  id_order_state int NOT NULL,
  id_lang int NOT NULL,
  name text NOT NULL,
  template text NOT NULL,
  PRIMARY KEY (id_order_state)
  -- PRIMARY KEY (id_order_state, id_lang)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Define which products / quantities define a pack. A product could be a pack */
CREATE TABLE PREFIX_pack (
  id_product_pack int NOT NULL,
  id_product_item int NOT NULL,
  id_product_attribute_item int NOT NULL,
  quantity int NOT NULL,
  PRIMARY KEY (id_product_pack)
  -- PRIMARY KEY (
  --   id_product_pack, id_product_item,
  --   id_product_attribute_item
  -- ),
  -- KEY product_item (
  --   id_product_item, id_product_attribute_item
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* page stats (PS_STATSDATA_CUSTOMER_PAGESVIEWS) */
CREATE TABLE PREFIX_page (
  id_page int NOT NULL,
  id_page_type int NOT NULL,
  id_object int,
  PRIMARY KEY (id_page)
  -- KEY id_page_type (id_page_type),
  -- KEY id_object (id_object)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* List of page type (stats) */
CREATE TABLE PREFIX_page_type (
  id_page_type int NOT NULL,
  name text NOT NULL,
  PRIMARY KEY (id_page_type)
  -- KEY name (name)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Page viewed (stats) */
CREATE TABLE PREFIX_page_viewed (
  id_page int NOT NULL,
  id_shop_group int NOT NULL,
  id_shop int NOT NULL,
  id_date_range int NOT NULL,
  counter int NOT NULL,
  PRIMARY KEY (id_page)
  -- PRIMARY KEY (
  --   id_page, id_date_range, id_shop
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Payment info (see payment_invoice) */
CREATE TABLE PREFIX_order_payment (
  id_order_payment int NOT NULL,
  order_reference text,
  id_currency int NOT NULL,
  amount int NOT NULL,
  payment_method text NOT NULL,
  conversion_rate int NOT NULL,
  transaction_id text NOT NULL,
  card_number text NOT NULL,
  card_brand text NOT NULL,
  card_expiration text NOT NULL,
  card_holder text NOT NULL,
  date_add datetime NOT NULL,
  PRIMARY KEY (id_order_payment)
  -- KEY order_reference(order_reference)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* list of products */
CREATE TABLE PREFIX_product (
  id_product int NOT NULL,
  id_supplier int,
  id_manufacturer int,
  id_category_default int,
  id_shop_default int NOT NULL,
  id_tax_rules_group int NOT NULL,
  on_sale int NOT NULL,
  online_only int NOT NULL,
  ean13 text,
  isbn text,
  upc text,
  mpn text,
  ecotax int NOT NULL,
  quantity int NOT NULL,
  minimal_quantity int NOT NULL,
  low_stock_threshold int NOT NULL,
  low_stock_alert int NOT NULL,
  price int NOT NULL,
  wholesale_price int NOT NULL,
  unity text,
  unit_price_ratio int NOT NULL,
  additional_shipping_cost int NOT NULL,
  reference text,
  supplier_reference text,
  location text,
  width int NOT NULL,
  height int NOT NULL,
  depth int NOT NULL,
  weight int NOT NULL,
  out_of_stock int NOT NULL,
  additional_delivery_times int NOT NULL,
  quantity_discount int,
  customizable int NOT NULL,
  uploadable_files int NOT NULL,
  text_fields int NOT NULL,
  active int NOT NULL,
  redirect_type text NOT NULL,
  id_type_redirected int NOT NULL,
  available_for_order int NOT NULL,
  available_date datetime,
  show_condition int NOT NULL,
  condition text NOT NULL,
  show_price int NOT NULL,
  -- indexed int NOT NULL,
  visibility text NOT NULL,
  cache_is_pack int NOT NULL,
  cache_has_attachments int NOT NULL,
  is_virtual int NOT NULL,
  cache_default_attribute int,
  date_add datetime NOT NULL,
  date_upd datetime NOT NULL,
  advanced_stock_management int NOT NULL,
  pack_stock_type int NOT NULL,
  state int NOT NULL,
  PRIMARY KEY (id_product)
  -- INDEX reference_idx(reference),
  -- INDEX supplier_reference_idx(supplier_reference),
  -- KEY product_supplier (id_supplier),
  -- KEY product_manufacturer (id_manufacturer, id_product),
  -- KEY id_category_default (id_category_default),
  -- KEY indexed (indexed),
  -- KEY date_add (date_add),
  -- KEY state (state, date_upd)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* shop specific product info */
CREATE TABLE PREFIX_product_shop (
  id_product int NOT NULL,
  id_shop int NOT NULL,
  id_category_default int,
  id_tax_rules_group int NOT NULL,
  on_sale int NOT NULL,
  online_only int NOT NULL,
  ecotax int NOT NULL,
  minimal_quantity int NOT NULL,
  low_stock_threshold int NOT NULL,
  low_stock_alert int NOT NULL,
  price int NOT NULL,
  wholesale_price int NOT NULL,
  unity text,
  unit_price_ratio int NOT NULL,
  additional_shipping_cost int NOT NULL,
  customizable int NOT NULL,
  uploadable_files int NOT NULL,
  text_fields int NOT NULL,
  active int NOT NULL,
  redirect_type text NOT NULL,
  id_type_redirected int NOT NULL,
  available_for_order int NOT NULL,
  available_date datetime,
  show_condition int NOT NULL,
  condition text NOT NULL,
  show_price int NOT NULL,
  -- indexed int NOT NULL,
  visibility text NOT NULL,
  cache_default_attribute int,
  advanced_stock_management int NOT NULL,
  date_add datetime NOT NULL,
  date_upd datetime NOT NULL,
  pack_stock_type int NOT NULL,
  PRIMARY KEY (id_product)
  -- PRIMARY KEY (id_product, id_shop),
  -- KEY id_category_default (id_category_default),
  -- KEY date_add (
  --   date_add, active, visibility
  -- ),
  -- KEY indexed (
  --   indexed, active, id_product
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* list of product attributes (E.g. : color) */
CREATE TABLE PREFIX_product_attribute (
  id_product_attribute int NOT NULL,
  id_product int NOT NULL,
  reference text,
  supplier_reference text,
  location text,
  ean13 text,
  isbn text,
  upc text,
  mpn text,
  wholesale_price int NOT NULL,
  price int NOT NULL,
  ecotax int NOT NULL,
  quantity int NOT NULL,
  weight int NOT NULL,
  unit_price_impact int NOT NULL,
  default_on int NOT NULL,
  minimal_quantity int NOT NULL,
  low_stock_threshold int NOT NULL,
  low_stock_alert int NOT NULL,
  available_date datetime,
  PRIMARY KEY (id_product_attribute)
  -- KEY product_attribute_product (id_product),
  -- KEY reference (reference),
  -- KEY supplier_reference (supplier_reference),
  -- UNIQUE KEY product_default (id_product, default_on),
  -- KEY id_product_id_product_attribute (
  --   id_product_attribute, id_product
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* shop specific attribute info */
CREATE TABLE PREFIX_product_attribute_shop (
  id_product int NOT NULL,
  id_product_attribute int NOT NULL,
  id_shop int NOT NULL,
  wholesale_price int NOT NULL,
  price int NOT NULL,
  ecotax int NOT NULL,
  weight int NOT NULL,
  unit_price_impact int NOT NULL,
  default_on int NOT NULL,
  minimal_quantity int NOT NULL,
  low_stock_threshold int NOT NULL,
  low_stock_alert int NOT NULL,
  available_date datetime,
  PRIMARY KEY (id_product_attribute)
  -- PRIMARY KEY (
  --   id_product_attribute, id_shop
  -- ),
  -- UNIQUE KEY id_product (
  --   id_product, id_shop, default_on
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* association between attribute and combination */
CREATE TABLE PREFIX_product_attribute_combination (
  id_attribute int NOT NULL,
  id_product_attribute int NOT NULL,
  PRIMARY KEY (id_attribute)
  -- PRIMARY KEY (
  --   id_attribute, id_product_attribute
  -- ),
  -- KEY id_product_attribute (id_product_attribute)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* image associated with an attribute */
CREATE TABLE PREFIX_product_attribute_image (
  id_product_attribute int NOT NULL,
  id_image int NOT NULL,
  PRIMARY KEY (id_product_attribute)
  -- PRIMARY KEY (
  --   id_product_attribute, id_image
  -- ),
  -- KEY id_image (id_image)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Virtual product download info */
CREATE TABLE PREFIX_product_download (
  id_product_download int NOT NULL,
  id_product int NOT NULL,
  display_filename text,
  filename text,
  date_add datetime NOT NULL,
  date_expiration datetime,
  nb_days_accessible int,
  nb_downloadable int,
  active int NOT NULL,
  is_shareable int NOT NULL,
  PRIMARY KEY (id_product_download)
  -- KEY product_active (id_product, active),
  -- UNIQUE KEY id_product (id_product)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Localized product info */
CREATE TABLE PREFIX_product_lang (
  id_product int NOT NULL,
  id_shop int NOT NULL,
  id_lang int NOT NULL,
  description text,
  description_short text,
  link_rewrite text NOT NULL,
  meta_description text,
  meta_keywords text,
  meta_title text,
  name text NOT NULL,
  available_now text,
  available_later text,
  delivery_in_stock text,
  delivery_out_stock text,
  PRIMARY KEY (id_product)
  -- PRIMARY KEY (
  --   id_product, id_shop, id_lang
  -- ),
  -- KEY id_lang (id_lang),
  -- KEY name (name)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* info about number of products sold */
CREATE TABLE PREFIX_product_sale (
  id_product int NOT NULL,
  quantity int NOT NULL,
  sale_nbr int NOT NULL,
  date_upd datetime,
  PRIMARY KEY (id_product)
  -- KEY quantity (quantity)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* tags associated with a product */
CREATE TABLE PREFIX_product_tag (
  id_product int NOT NULL,
  id_tag int NOT NULL,
  id_lang int NOT NULL,
  PRIMARY KEY (id_product)
  -- PRIMARY KEY (id_product, id_tag),
  -- KEY id_tag (id_tag),
  -- KEY id_lang (id_lang, id_tag)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* List of profile (admin, superadmin, etc...) */
CREATE TABLE PREFIX_profile (
  id_profile int NOT NULL,
  PRIMARY KEY (id_profile)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Localized profile names */
CREATE TABLE PREFIX_profile_lang (
  id_lang int NOT NULL,
  id_profile int NOT NULL,
  name text NOT NULL,
  PRIMARY KEY (id_profile)
  -- PRIMARY KEY (id_profile, id_lang)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* List of quick access link used in the admin */
CREATE TABLE PREFIX_quick_access (
  id_quick_access int NOT NULL,
  new_window int NOT NULL,
  link text NOT NULL,
  PRIMARY KEY (id_quick_access)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Localized quick access names */
CREATE TABLE PREFIX_quick_access_lang (
  id_quick_access int NOT NULL,
  id_lang int NOT NULL,
  name text NOT NULL,
  PRIMARY KEY (id_quick_access)
  -- PRIMARY KEY (id_quick_access, id_lang)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* price ranges used for delivery */
CREATE TABLE PREFIX_range_price (
  id_range_price int NOT NULL,
  id_carrier int NOT NULL,
  delimiter1 int NOT NULL,
  delimiter2 int NOT NULL,
  PRIMARY KEY (id_range_price)
  -- UNIQUE KEY id_carrier (
  --   id_carrier, delimiter1, delimiter2
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Weight ranges used for delivery */
CREATE TABLE PREFIX_range_weight (
  id_range_weight int NOT NULL,
  id_carrier int NOT NULL,
  delimiter1 int NOT NULL,
  delimiter2 int NOT NULL,
  PRIMARY KEY (id_range_weight)
  -- UNIQUE KEY id_carrier (
  --   id_carrier, delimiter1, delimiter2
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Referrer stats */
CREATE TABLE PREFIX_referrer (
  id_referrer int NOT NULL,
  name text NOT NULL,
  passwd text,
  http_referer_regexp text,
  http_referer_like text,
  request_uri_regexp text,
  request_uri_like text,
  http_referer_regexp_not text,
  http_referer_like_not text,
  request_uri_regexp_not text,
  request_uri_like_not text,
  base_fee int NOT NULL,
  percent_fee int NOT NULL,
  click_fee int NOT NULL,
  date_add datetime NOT NULL,
  PRIMARY KEY (id_referrer)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Referrer cache (stats) */
CREATE TABLE PREFIX_referrer_cache (
  id_connections_source int NOT NULL,
  id_referrer int NOT NULL,
  PRIMARY KEY (id_connections_source)
  -- PRIMARY KEY (
  --   id_connections_source, id_referrer
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Referrer shop info (stats) */
CREATE TABLE PREFIX_referrer_shop (
  id_referrer int NOT NULL,
  id_shop int NOT NULL,
  cache_visitors int,
  cache_visits int,
  cache_pages int,
  cache_registrations int,
  cache_orders int,
  cache_sales int,
  cache_reg_rate int,
  cache_order_rate int,
  PRIMARY KEY (id_referrer)
  -- PRIMARY KEY (id_referrer, id_shop)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* List of custom SQL request saved on the admin (used to generate exports) */
CREATE TABLE PREFIX_request_sql (
  id_request_sql int NOT NULL,
  name text NOT NULL,
  sql text NOT NULL,
  PRIMARY KEY (id_request_sql)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* List of search engine + query string (used by SEO module) */
CREATE TABLE PREFIX_search_engine (
  id_search_engine int NOT NULL,
  server text NOT NULL,
  getvar text NOT NULL,
  PRIMARY KEY (id_search_engine)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Index constructed by the search engine */
CREATE TABLE PREFIX_search_index (
  id_product int NOT NULL,
  id_word int NOT NULL,
  weight int NOT NULL,
  PRIMARY KEY (id_word)
  -- PRIMARY KEY (id_word, id_product),
  -- KEY id_product (id_product, weight)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* List of words available for a given shop & lang */
CREATE TABLE PREFIX_search_word (
  id_word int NOT NULL,
  id_shop int NOT NULL,
  id_lang int NOT NULL,
  word text NOT NULL,
  PRIMARY KEY (id_word)
  -- UNIQUE KEY id_lang (id_lang, id_shop, word)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* List of price reduction depending on given conditions */
CREATE TABLE PREFIX_specific_price (
  id_specific_price int NOT NULL,
  id_specific_price_rule int NOT NULL,
  id_cart int NOT NULL,
  id_product int NOT NULL,
  id_shop int NOT NULL,
  id_shop_group int NOT NULL,
  id_currency int NOT NULL,
  id_country int NOT NULL,
  id_group int NOT NULL,
  id_customer int NOT NULL,
  id_product_attribute int NOT NULL,
  price int NOT NULL,
  from_quantity int NOT NULL,
  reduction int NOT NULL,
  reduction_tax int NOT NULL,
  reduction_type text NOT NULL,
  -- from datetime NOT NULL,
  -- to datetime NOT NULL,
  PRIMARY KEY (id_specific_price)
  -- KEY (
  --   id_product, id_shop, id_currency,
  --   id_country, id_group, id_customer,
  --   from_quantity, from, to
  -- ),
  -- KEY from_quantity (from_quantity),
  -- KEY (id_specific_price_rule),
  -- KEY (id_cart),
  -- KEY id_product_attribute (id_product_attribute),
  -- KEY id_shop (id_shop),
  -- KEY id_customer (id_customer),
  -- KEY from (from),
  -- KEY to (to),
  -- UNIQUE KEY id_product_2 (
  --   id_product, id_product_attribute,
  --   id_customer, id_cart, from,
  --   to, id_shop, id_shop_group,
  --   id_currency, id_country, id_group,
  --   from_quantity, id_specific_price_rule
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* State localization info */
CREATE TABLE PREFIX_state (
  id_state int NOT NULL,
  id_country int NOT NULL,
  id_zone int NOT NULL,
  name text NOT NULL,
  iso_code text NOT NULL,
  tax_behavior int NOT NULL,
  active int NOT NULL,
  PRIMARY KEY (id_state)
  -- KEY id_country (id_country),
  -- KEY name (name),
  -- KEY id_zone (id_zone)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* List of suppliers */
CREATE TABLE PREFIX_supplier (
  id_supplier int NOT NULL,
  name text NOT NULL,
  date_add datetime NOT NULL,
  date_upd datetime NOT NULL,
  active int NOT NULL,
  PRIMARY KEY (id_supplier)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Localized supplier data */
CREATE TABLE PREFIX_supplier_lang (
  id_supplier int NOT NULL,
  id_lang int NOT NULL,
  description text,
  meta_title text,
  meta_keywords text,
  meta_description text,
  PRIMARY KEY (id_supplier)
  -- PRIMARY KEY (id_supplier, id_lang)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* List of tags */
CREATE TABLE PREFIX_tag (
  id_tag int NOT NULL,
  id_lang int NOT NULL,
  name text NOT NULL,
  PRIMARY KEY (id_tag)
  -- KEY tag_name (name),
  -- KEY id_lang (id_lang)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Count info associated with each tag depending on lang, group & shop (cloud tags) */
CREATE TABLE PREFIX_tag_count (
  id_group int NOT NULL,
  id_tag int NOT NULL,
  id_lang int NOT NULL,
  id_shop int NOT NULL,
  counter int NOT NULL,
  PRIMARY KEY (id_group)
  -- KEY (
  --   id_group, id_lang, id_shop,
  --   counter
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* List of taxes */
CREATE TABLE PREFIX_tax (
  id_tax int NOT NULL,
  rate int NOT NULL,
  active int NOT NULL,
  deleted int NOT NULL,
  PRIMARY KEY (id_tax)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Localized tax names */
CREATE TABLE PREFIX_tax_lang (
  id_tax int NOT NULL,
  id_lang int NOT NULL,
  name text NOT NULL,
  PRIMARY KEY (id_tax)
  -- PRIMARY KEY (id_tax, id_lang)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* List of timezone */
CREATE TABLE PREFIX_timezone (
  id_timezone int NOT NULL,
  name text NOT NULL,
  PRIMARY KEY (id_timezone)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* List of web browsers */
CREATE TABLE PREFIX_web_browser (
  id_web_browser int NOT NULL,
  name text,
  PRIMARY KEY (id_web_browser)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* List of geographic zones */
CREATE TABLE PREFIX_zone (
  id_zone int NOT NULL,
  name text NOT NULL,
  active int NOT NULL,
  PRIMARY KEY (id_zone)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Carrier available for a specific group */
CREATE TABLE PREFIX_carrier_group (
  id_carrier int NOT NULL,
  id_group int NOT NULL,
  PRIMARY KEY (id_carrier)
  -- PRIMARY KEY (id_carrier, id_group)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* List of stores */
CREATE TABLE PREFIX_store (
  id_store int NOT NULL,
  id_country int NOT NULL,
  id_state int,
  city text NOT NULL,
  postcode text NOT NULL,
  latitude int,
  longitude int,
  phone text,
  fax text,
  email text,
  active int NOT NULL,
  date_add datetime NOT NULL,
  date_upd datetime NOT NULL,
  PRIMARY KEY (id_store)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_store_lang (
  id_store int NOT NULL,
  id_lang int NOT NULL,
  name text NOT NULL,
  address1 text NOT NULL,
  address2 text,
  hours text,
  note text,
  PRIMARY KEY (id_store)
  -- PRIMARY KEY (id_store, id_lang)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Webservice account infos */
CREATE TABLE PREFIX_webservice_account (
  id_webservice_account int NOT NULL,
  -- key text NOT NULL,
  description text NOT NULL,
  class_name text NOT NULL,
  is_module int NOT NULL,
  module_name text NOT NULL,
  active int NOT NULL,
  PRIMARY KEY (id_webservice_account)
  -- KEY key (key)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

/* Permissions associated with a webservice account */
CREATE TABLE PREFIX_webservice_permission (
  id_webservice_permission int NOT NULL,
  resource text NOT NULL,
  method text NOT NULL,
  id_webservice_account int NOT NULL,
  PRIMARY KEY (id_webservice_permission)
  -- UNIQUE KEY resource_2 (
  --   resource, method, id_webservice_account
  -- ),
  -- KEY resource (resource),
  -- KEY method (method),
  -- KEY id_webservice_account (id_webservice_account)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_required_field (
  id_required_field int NOT NULL,
  object_name text NOT NULL,
  field_name text NOT NULL,
  PRIMARY KEY (id_required_field)
  -- KEY object_name (object_name)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_memcached_servers (
  id_memcached_server int NOT NULL PRIMARY KEY,
  ip text NOT NULL,
  port int NOT NULL,
  weight int NOT NULL
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_product_country_tax (
  id_product int NOT NULL,
  id_country int NOT NULL,
  id_tax int NOT NULL,
  PRIMARY KEY (id_product)
  -- PRIMARY KEY (id_product, id_country)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_tax_rule (
  id_tax_rule int NOT NULL,
  id_tax_rules_group int NOT NULL,
  id_country int NOT NULL,
  id_state int NOT NULL,
  zipcode_from text NOT NULL,
  zipcode_to text NOT NULL,
  id_tax int NOT NULL,
  behavior int NOT NULL,
  description text NOT NULL,
  PRIMARY KEY (id_tax_rule)
  -- KEY id_tax_rules_group (id_tax_rules_group),
  -- KEY id_tax (id_tax),
  -- KEY category_getproducts (
  --   id_tax_rules_group, id_country,
  --   id_state, zipcode_from
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_tax_rules_group (
  id_tax_rules_group int NOT NULL PRIMARY KEY,
  name text NOT NULL,
  active int NOT NULL,
  deleted int NOT NULL,
  date_add datetime NOT NULL,
  date_upd datetime NOT NULL
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_specific_price_priority (
  id_specific_price_priority int NOT NULL,
  id_product int NOT NULL,
  priority text NOT NULL,
  PRIMARY KEY (id_specific_price_priority)
  -- PRIMARY KEY (
  --   id_specific_price_priority, id_product
  -- ),
  -- UNIQUE KEY id_product (id_product)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_log (
  id_log int NOT NULL,
  severity int NOT NULL,
  error_code int,
  message text NOT NULL,
  object_type text,
  object_id int,
  id_employee int,
  date_add datetime NOT NULL,
  date_upd datetime NOT NULL,
  PRIMARY KEY (id_log)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_import_match (
  id_import_match int NOT NULL,
  name text NOT NULL,
  -- match text NOT NULL,
  skip int NOT NULL,
  PRIMARY KEY (id_import_match)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_shop_url (
  id_shop_url int NOT NULL,
  id_shop int NOT NULL,
  domain text NOT NULL,
  domain_ssl text NOT NULL,
  physical_uri text NOT NULL,
  virtual_uri text NOT NULL,
  main int NOT NULL,
  active int NOT NULL,
  PRIMARY KEY (id_shop_url)
  -- KEY id_shop (id_shop, main),
  -- UNIQUE KEY full_shop_url (
  --   domain, physical_uri, virtual_uri
  -- ),
  -- UNIQUE KEY full_shop_url_ssl (
  --   domain_ssl, physical_uri, virtual_uri
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_country_shop (
  id_country int NOT NULL,
  id_shop int NOT NULL,
  PRIMARY KEY (id_country)
  -- PRIMARY KEY (id_country, id_shop),
  -- KEY id_shop (id_shop)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_carrier_shop (
  id_carrier int NOT NULL,
  id_shop int NOT NULL,
  PRIMARY KEY (id_carrier)
  -- PRIMARY KEY (id_carrier, id_shop),
  -- KEY id_shop (id_shop)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_address_format (
  id_country int NOT NULL,
  format text NOT NULL,
  PRIMARY KEY (id_country)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_cms_shop (
  id_cms int NOT NULL,
  id_shop int NOT NULL,
  PRIMARY KEY (id_cms)
  -- PRIMARY KEY (id_cms, id_shop),
  -- KEY id_shop (id_shop)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_currency_shop (
  id_currency int NOT NULL,
  id_shop int NOT NULL,
  conversion_rate int NOT NULL,
  PRIMARY KEY (id_currency)
  -- PRIMARY KEY (id_currency, id_shop),
  -- KEY id_shop (id_shop)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_contact_shop (
  id_contact int NOT NULL,
  id_shop int NOT NULL,
  PRIMARY KEY (id_contact)
  -- PRIMARY KEY (id_contact, id_shop),
  -- KEY id_shop (id_shop)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_image_shop (
  id_product int NOT NULL,
  id_image int NOT NULL,
  id_shop int NOT NULL,
  cover int NOT NULL,
  PRIMARY KEY (id_image)
  -- PRIMARY KEY (id_image, id_shop),
  -- UNIQUE KEY id_product (id_product, id_shop, cover),
  -- KEY id_shop (id_shop)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_feature_shop (
  id_feature int NOT NULL,
  id_shop int NOT NULL,
  PRIMARY KEY (id_feature)
  -- PRIMARY KEY (id_feature, id_shop),
  -- KEY id_shop (id_shop)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_group_shop (
  id_group int NOT NULL,
  id_shop int NOT NULL,
  PRIMARY KEY (id_group)
  -- PRIMARY KEY (id_group, id_shop),
  -- KEY id_shop (id_shop)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_tax_rules_group_shop (
  id_tax_rules_group int NOT NULL,
  id_shop int NOT NULL,
  PRIMARY KEY (id_tax_rules_group)
  -- PRIMARY KEY (id_tax_rules_group, id_shop),
  -- KEY id_shop (id_shop)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_zone_shop (
  id_zone int NOT NULL,
  id_shop int NOT NULL,
  PRIMARY KEY (id_zone)
  -- PRIMARY KEY (id_zone, id_shop),
  -- KEY id_shop (id_shop)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_manufacturer_shop (
  id_manufacturer int NOT NULL,
  id_shop int NOT NULL,
  PRIMARY KEY (id_manufacturer)
  -- PRIMARY KEY (id_manufacturer, id_shop),
  -- KEY id_shop (id_shop)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_supplier_shop (
  id_supplier int NOT NULL,
  id_shop int NOT NULL,
  PRIMARY KEY (id_supplier)
  -- PRIMARY KEY (id_supplier, id_shop),
  -- KEY id_shop (id_shop)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_store_shop (
  id_store int NOT NULL,
  id_shop int NOT NULL,
  PRIMARY KEY (id_store)
  -- PRIMARY KEY (id_store, id_shop),
  -- KEY id_shop (id_shop)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_module_shop (
  id_module int NOT NULL,
  id_shop int NOT NULL,
  enable_device int NOT NULL,
  PRIMARY KEY (id_module)
  -- PRIMARY KEY (id_module, id_shop),
  -- KEY id_shop (id_shop)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_webservice_account_shop (
  id_webservice_account int NOT NULL,
  id_shop int NOT NULL,
  PRIMARY KEY (id_webservice_account)
  -- PRIMARY KEY (
  --   id_webservice_account, id_shop
  -- ),
  -- KEY id_shop (id_shop)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_stock_mvt_reason (
  id_stock_mvt_reason int NOT NULL,
  sign int NOT NULL,
  date_add datetime NOT NULL,
  date_upd datetime NOT NULL,
  deleted int NOT NULL,
  PRIMARY KEY (id_stock_mvt_reason)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_stock_mvt_reason_lang (
  id_stock_mvt_reason int NOT NULL,
  id_lang int NOT NULL,
  name text NOT NULL,
  PRIMARY KEY (id_stock_mvt_reason)
  -- PRIMARY KEY (
  --   id_stock_mvt_reason, id_lang
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_stock (
  id_stock int NOT NULL,
  id_warehouse int NOT NULL,
  id_product int NOT NULL,
  id_product_attribute int NOT NULL,
  reference text NOT NULL,
  ean13 text,
  isbn text,
  upc text,
  mpn text,
  physical_quantity int NOT NULL,
  usable_quantity int NOT NULL,
  price_te int,
  PRIMARY KEY (id_stock)
  -- KEY id_warehouse (id_warehouse),
  -- KEY id_product (id_product),
  -- KEY id_product_attribute (id_product_attribute)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_warehouse (
  id_warehouse int NOT NULL,
  id_currency int NOT NULL,
  id_address int NOT NULL,
  id_employee int NOT NULL,
  reference text,
  name text NOT NULL,
  management_type text NOT NULL,
  deleted int NOT NULL,
  PRIMARY KEY (id_warehouse)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_warehouse_product_location (
  id_warehouse_product_location int NOT NULL,
  id_product int NOT NULL,
  id_product_attribute int NOT NULL,
  id_warehouse int NOT NULL,
  location text,
  PRIMARY KEY (id_warehouse_product_location)
  -- UNIQUE KEY id_product (
  --   id_product, id_product_attribute,
  --   id_warehouse
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_warehouse_shop (
  id_shop int NOT NULL,
  id_warehouse int NOT NULL,
  PRIMARY KEY (id_warehouse)
  -- PRIMARY KEY (id_warehouse, id_shop),
  -- KEY id_warehouse (id_warehouse),
  -- KEY id_shop (id_shop)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_warehouse_carrier (
  id_carrier int NOT NULL,
  id_warehouse int NOT NULL,
  PRIMARY KEY (id_warehouse)
  -- PRIMARY KEY (id_warehouse, id_carrier),
  -- KEY id_warehouse (id_warehouse),
  -- KEY id_carrier (id_carrier)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_stock_available (
  id_stock_available int NOT NULL,
  id_product int NOT NULL,
  id_product_attribute int NOT NULL,
  id_shop int NOT NULL,
  id_shop_group int NOT NULL,
  quantity int NOT NULL,
  physical_quantity int NOT NULL,
  reserved_quantity int NOT NULL,
  depends_on_stock int NOT NULL,
  out_of_stock int NOT NULL,
  location text NOT NULL,
  PRIMARY KEY (id_stock_available)
  -- KEY id_shop (id_shop),
  -- KEY id_shop_group (id_shop_group),
  -- KEY id_product (id_product),
  -- KEY id_product_attribute (id_product_attribute),
  -- UNIQUE product_sqlstock (
  --   id_product, id_product_attribute,
  --   id_shop, id_shop_group
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_supply_order (
  id_supply_order int NOT NULL,
  id_supplier int NOT NULL,
  supplier_name text NOT NULL,
  id_lang int NOT NULL,
  id_warehouse int NOT NULL,
  id_supply_order_state int NOT NULL,
  id_currency int NOT NULL,
  id_ref_currency int NOT NULL,
  reference text NOT NULL,
  date_add datetime NOT NULL,
  date_upd datetime NOT NULL,
  date_delivery_expected datetime,
  total_te int,
  total_with_discount_te int,
  total_tax int,
  total_ti int,
  discount_rate int,
  discount_value_te int,
  is_template int,
  PRIMARY KEY (id_supply_order)
  -- KEY id_supplier (id_supplier),
  -- KEY id_warehouse (id_warehouse),
  -- KEY reference (reference)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_supply_order_detail (
  id_supply_order_detail int NOT NULL,
  id_supply_order int NOT NULL,
  id_currency int NOT NULL,
  id_product int NOT NULL,
  id_product_attribute int NOT NULL,
  reference text NOT NULL,
  supplier_reference text NOT NULL,
  name text NOT NULL,
  ean13 text,
  isbn text,
  upc text,
  mpn text,
  exchange_rate int,
  unit_price_te int,
  quantity_expected int NOT NULL,
  quantity_received int NOT NULL,
  price_te int,
  discount_rate int,
  discount_value_te int,
  price_with_discount_te int,
  tax_rate int,
  tax_value int,
  price_ti int,
  tax_value_with_order_discount int,
  price_with_order_discount_te int,
  PRIMARY KEY (id_supply_order_detail)
  -- KEY id_supply_order (id_supply_order, id_product),
  -- KEY id_product_attribute (id_product_attribute),
  -- KEY id_product_product_attribute (
  --   id_product, id_product_attribute
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_supply_order_history (
  id_supply_order_history int NOT NULL,
  id_supply_order int NOT NULL,
  id_employee int NOT NULL,
  employee_lastname text,
  employee_firstname text,
  id_state int NOT NULL,
  date_add datetime NOT NULL,
  PRIMARY KEY (id_supply_order_history)
  -- KEY id_supply_order (id_supply_order),
  -- KEY id_employee (id_employee),
  -- KEY id_state (id_state)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_supply_order_state (
  id_supply_order_state int NOT NULL,
  delivery_note int NOT NULL,
  editable int NOT NULL,
  receipt_state int NOT NULL,
  pending_receipt int NOT NULL,
  enclosed int NOT NULL,
  color text,
  PRIMARY KEY (id_supply_order_state)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_supply_order_state_lang (
  id_supply_order_state int NOT NULL,
  id_lang int NOT NULL,
  name text,
  PRIMARY KEY (id_supply_order_state)
  -- PRIMARY KEY (
  --   id_supply_order_state, id_lang
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_supply_order_receipt_history (
  id_supply_order_receipt_history int NOT NULL,
  id_supply_order_detail int NOT NULL,
  id_employee int NOT NULL,
  employee_lastname text,
  employee_firstname text,
  id_supply_order_state int NOT NULL,
  quantity int NOT NULL,
  date_add datetime NOT NULL,
  PRIMARY KEY (
    id_supply_order_receipt_history
  )
  -- KEY id_supply_order_detail (id_supply_order_detail),
  -- KEY id_supply_order_state (id_supply_order_state)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_product_supplier (
  id_product_supplier int NOT NULL,
  id_product int NOT NULL,
  id_product_attribute int NOT NULL,
  id_supplier int NOT NULL,
  product_supplier_reference text,
  product_supplier_price_te int NOT NULL,
  id_currency int NOT NULL,
  PRIMARY KEY (id_product_supplier)
  -- UNIQUE KEY id_product (
  --   id_product, id_product_attribute,
  --   id_supplier
  -- ),
  -- KEY id_supplier (id_supplier, id_product)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_order_carrier (
  id_order_carrier int NOT NULL,
  id_order int NOT NULL,
  id_carrier int NOT NULL,
  id_order_invoice int,
  weight int,
  shipping_cost_tax_excl int,
  shipping_cost_tax_incl int,
  tracking_number text,
  date_add datetime NOT NULL,
  PRIMARY KEY (id_order_carrier)
  -- KEY id_order (id_order),
  -- KEY id_carrier (id_carrier),
  -- KEY id_order_invoice (id_order_invoice)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_specific_price_rule (
  id_specific_price_rule int NOT NULL,
  name text NOT NULL,
  id_shop int NOT NULL,
  id_currency int NOT NULL,
  id_country int NOT NULL,
  id_group int NOT NULL,
  from_quantity int NOT NULL,
  price int,
  reduction int NOT NULL,
  reduction_tax int NOT NULL,
  reduction_type text NOT NULL,
  -- from datetime NOT NULL,
  -- to datetime NOT NULL,
  PRIMARY KEY (id_specific_price_rule)
  -- KEY id_product (
  --   id_shop, id_currency, id_country,
  --   id_group, from_quantity, from,
  --   to
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_specific_price_rule_condition_group (
  id_specific_price_rule_condition_group int NOT NULL,
  id_specific_price_rule int NOT NULL,
  PRIMARY KEY (id_specific_price_rule_condition_group)
  -- PRIMARY KEY (
  --   id_specific_price_rule_condition_group,
  --   id_specific_price_rule
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_specific_price_rule_condition (
  id_specific_price_rule_condition int NOT NULL,
  id_specific_price_rule_condition_group int NOT NULL,
  type text NOT NULL,
  value text NOT NULL,
  PRIMARY KEY (id_specific_price_rule_condition)
  -- INDEX (
    -- id_specific_price_rule_condition_group
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_risk (
  id_risk int NOT NULL,
  percent int NOT NULL,
  color text NOT NULL,
  PRIMARY KEY (id_risk)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_risk_lang (
  id_risk int NOT NULL,
  id_lang int NOT NULL,
  name text NOT NULL,
  PRIMARY KEY (id_risk)
  -- PRIMARY KEY (id_risk, id_lang),
  -- KEY id_risk (id_risk)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_category_shop (
  id_category int NOT NULL,
  id_shop int NOT NULL,
  position int NOT NULL,
  PRIMARY KEY (id_category)
  -- PRIMARY KEY (id_category, id_shop)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_module_preference (
  id_module_preference int NOT NULL,
  id_employee int NOT NULL,
  module text NOT NULL,
  interest int,
  favorite int,
  PRIMARY KEY (id_module_preference)
  -- UNIQUE KEY employee_module (id_employee, module)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_tab_module_preference (
  id_tab_module_preference int NOT NULL,
  id_employee int NOT NULL,
  id_tab int NOT NULL,
  module text NOT NULL,
  PRIMARY KEY (id_tab_module_preference)
  -- UNIQUE KEY employee_module (
  --   id_employee, id_tab, module
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_carrier_tax_rules_group_shop (
  id_carrier int NOT NULL,
  id_tax_rules_group int NOT NULL,
  id_shop int NOT NULL,
  PRIMARY KEY (id_carrier)
  -- PRIMARY KEY (
  --   id_carrier, id_tax_rules_group,
  --   id_shop
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_order_invoice_payment (
  id_order_invoice int NOT NULL,
  id_order_payment int NOT NULL,
  id_order int NOT NULL,
  PRIMARY KEY (id_order_invoice)
  -- PRIMARY KEY (
  --   id_order_invoice, id_order_payment
  -- ),
  -- KEY order_payment (id_order_payment),
  -- KEY id_order (id_order)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_smarty_cache (
  id_smarty_cache text NOT NULL,
  name text NOT NULL,
  cache_id text,
  modified datetime NOT NULL,
  content text NOT NULL,
  PRIMARY KEY (id_smarty_cache)
  -- KEY name (name),
  -- KEY cache_id (cache_id),
  -- KEY modified (modified)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_order_slip_detail_tax (
  id_order_slip_detail int NOT NULL,
  id_tax int NOT NULL,
  unit_amount int NOT NULL,
  total_amount int NOT NULL,
  PRIMARY KEY (id_order_slip_detail)
  -- KEY (id_order_slip_detail),
  -- KEY id_tax (id_tax)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_mail (
  id_mail int NOT NULL,
  recipient text NOT NULL,
  template text NOT NULL,
  subject text NOT NULL,
  id_lang int NOT NULL,
  date_add datetime NOT NULL,
  PRIMARY KEY (id_mail)
  -- KEY recipient (
  --   recipient(10)
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_smarty_lazy_cache (
  template_hash text NOT NULL,
  cache_id text NOT NULL,
  compile_id text NOT NULL,
  filepath text NOT NULL,
  last_update datetime NOT NULL,
  PRIMARY KEY (template_hash)
  -- PRIMARY KEY (
  --   template_hash, cache_id, compile_id
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4;

CREATE TABLE PREFIX_smarty_last_flush (
  type text,
  last_flush datetime NOT NULL,
  PRIMARY KEY (type)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4;

CREATE TABLE PREFIX_cms_role (
  id_cms_role int NOT NULL,
  name text NOT NULL,
  id_cms int NOT NULL,
  PRIMARY KEY (id_cms_role)
  -- PRIMARY KEY (id_cms_role, id_cms),
  -- UNIQUE KEY name (name)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4;

CREATE TABLE PREFIX_cms_role_lang (
  id_cms_role int NOT NULL,
  id_lang int NOT NULL,
  id_shop int NOT NULL,
  name text,
  PRIMARY KEY (id_cms_role)
  -- PRIMARY KEY (
  --   id_cms_role, id_lang, id_shop
  -- )
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4;

CREATE TABLE PREFIX_employee_session (
  id_employee_session int NOT NULL,
  id_employee int,
  token text,
  PRIMARY KEY (id_employee_session)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

CREATE TABLE PREFIX_customer_session (
  id_customer_session int NOT NULL,
  id_customer int,
  token text,
  PRIMARY KEY (id_customer_session)
) ENGINE=ENGINE_TYPE DEFAULT CHARSET=utf8mb4 COLLATION;

EXPLAIN COMPLIANCE;