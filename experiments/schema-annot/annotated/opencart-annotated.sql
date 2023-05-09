CREATE TABLE user (
	user_id int NOT NULL,
	user_group_id int NOT NULL,
	username text NOT NULL,
	password text NOT NULL,
	salt text NOT NULL,
	firstname text NOT NULL,
	lastname text NOT NULL,
	email text NOT NULL,
	image text NOT NULL,
	code text NOT NULL,
	ip text NOT NULL,
	status int NOT NULL,
	date_added datetime NOT NULL,
	PRIMARY KEY (user_id)
);

CREATE DATA_SUBJECT TABLE customer (
	customer_id int NOT NULL,
	customer_group_id int NOT NULL,
	store_id int NOT NULL,
	language_id int NOT NULL,
	firstname text NOT NULL,
	lastname text NOT NULL,
	email text NOT NULL,
	telephone text NOT NULL,
	fax text NOT NULL,
	password text NOT NULL,
	salt text NOT NULL,
	cart text NOT NULL,
	wishlist text NOT NULL,
	newsletter int NOT NULL,
	address_id int NOT NULL,
	custom_field text NOT NULL,
	ip text NOT NULL,
	status int NOT NULL,
	safe int NOT NULL,
	token text NOT NULL,
	code text NOT NULL,
	date_added datetime NOT NULL,
	PRIMARY KEY (customer_id)
);

CREATE TABLE address (
    address_id int NOT NULL,
    customer_id int NOT NULL,
    firstname text NOT NULL,
    lastname text NOT NULL,
    company text NOT NULL,
    address_1 text NOT NULL,
    address_2 text NOT NULL,
    city text NOT NULL,
    postcode text NOT NULL,
    country_id int NOT NULL,
    zone_id int NOT NULL,
    custom_field text NOT NULL,
    PRIMARY KEY (address_id),
	FOREIGN KEY (customer_id) OWNED_BY customer(customer_id)
);

CREATE TABLE api (
    api_id int NOT NULL,
    username text NOT NULL,
    -- key text NOT NULL,
    status int NOT NULL,
    date_added datetime NOT NULL,
    date_modified datetime NOT NULL,
    PRIMARY KEY (api_id),
	-- FOREIGN KEY (username) REFERENCES users (user_id)
);

CREATE TABLE api_ip (
    api_ip_id int NOT NULL,
    api_id int NOT NULL,
    ip text NOT NULL,
    PRIMARY KEY (api_ip_id)
);

CREATE TABLE api_session (
    api_session_id int NOT NULL,
    api_id int NOT NULL,
    session_id text NOT NULL,
    session_name text NOT NULL,
    ip text NOT NULL,
    date_added datetime NOT NULL,
    date_modified datetime NOT NULL,
    PRIMARY KEY (api_session_id)
);

CREATE TABLE attribute (
    attribute_id int NOT NULL,
    attribute_group_id int NOT NULL,
    sort_order int NOT NULL,
    PRIMARY KEY (attribute_id)
);

CREATE TABLE attribute_description (
    attribute_id int NOT NULL,
    language_id int NOT NULL,
    name text NOT NULL,
    PRIMARY KEY (attribute_id)
);

CREATE TABLE attribute_group (
    attribute_group_id int NOT NULL,
    sort_order int NOT NULL,
    PRIMARY KEY (attribute_group_id)
);

CREATE TABLE attribute_group_description (
    attribute_group_id int NOT NULL,
    language_id int NOT NULL,
    name text NOT NULL,
    PRIMARY KEY (attribute_group_id)
);

CREATE TABLE banner (
    banner_id int NOT NULL,
    name text NOT NULL,
    status int NOT NULL,
    PRIMARY KEY (banner_id)
);

CREATE TABLE banner_image (
    banner_image_id int NOT NULL,
    banner_id int NOT NULL,
    link text NOT NULL,
    image text NOT NULL,
    sort_order int NOT NULL,
    PRIMARY KEY (banner_image_id)
);

CREATE TABLE cart (
    cart_id int NOT NULL,
    api_id int NOT NULL,
    customer_id int NOT NULL,
    session_id text NOT NULL,
    product_id int NOT NULL,
    recurring_id int NOT NULL,
    option text NOT NULL,
    quantity int NOT NULL,
    date_added datetime NOT NULL,
    PRIMARY KEY (cart_id),
	FOREIGN KEY (customer_id) REFERENCES customer(customer_id)
);

CREATE TABLE category (
    category_id int NOT NULL,
    image text NOT NULL,
    parent_id int NOT NULL,
    top int NOT NULL,
    -- column int NOT NULL,
    sort_order int NOT NULL,
    status int NOT NULL,
    date_added datetime NOT NULL,
    date_modified datetime NOT NULL,
    PRIMARY KEY (category_id)
);

CREATE TABLE category_description (
    category_id int NOT NULL,
    language_id int NOT NULL,
    name text NOT NULL,
    description text NOT NULL,
    meta_title text NOT NULL,
    meta_description text NOT NULL,
    meta_keyword text NOT NULL,
    PRIMARY KEY (category_id)
);

CREATE TABLE country (
    country_id int NOT NULL,
    name text NOT NULL,
    iso_code_2 text NOT NULL,
    iso_code_3 text NOT NULL,
    address_format text NOT NULL,
    postcode_required int NOT NULL,
    status int NOT NULL,
    PRIMARY KEY (country_id)
);

CREATE TABLE coupon (
    coupon_id int NOT NULL,
    name text NOT NULL,
    code text NOT NULL,
    type text NOT NULL,
    discount text NOT NULL,
    logged int NOT NULL,
    shipping int NOT NULL,
    total text NOT NULL,
    date_start datetime NOT NULL,
    date_end datetime NOT NULL,
    uses_total int NOT NULL,
    uses_customer int NOT NULL,
    status int NOT NULL,
    date_added datetime NOT NULL,
    PRIMARY KEY (coupon_id)
);

CREATE TABLE coupon_history (
    coupon_history_id int NOT NULL,
    coupon_id int NOT NULL,
    order_id int NOT NULL,
    customer_id int NOT NULL,
    amount text NOT NULL,
    date_added datetime NOT NULL,
    PRIMARY KEY (coupon_history_id),
	FOREIGN KEY (customer_id) REFERENCES customer(customer_id)
);

CREATE TABLE currency (
    currency_id int NOT NULL,
    title text NOT NULL,
    code text NOT NULL,
    symbol_left text NOT NULL,
    symbol_right text NOT NULL,
    decimal_place int NOT NULL,
    value text NOT NULL,
    status int NOT NULL,
    date_modified datetime NOT NULL,
    PRIMARY KEY (currency_id)
);

CREATE TABLE customer_activity (
	customer_activity_id int NOT NULL,
	customer_id int NOT NULL,
	-- key text NOT NULL,
	data text NOT NULL,
	ip text NOT NULL,
	date_added datetime NOT NULL,
	PRIMARY KEY (customer_activity_id),
	FOREIGN KEY (customer_id) OWNED_BY customer(customer_id)
);

CREATE TABLE customer_affiliate (
	customer_id int NOT NULL,
	company text NOT NULL,
	website text NOT NULL,
	tracking text NOT NULL,
	commission int NOT NULL,
	tax int NOT NULL,
	payment text NOT NULL,
	cheque text NOT NULL,
	paypal text NOT NULL,
	bank_name text NOT NULL,
	bank_branch_number text NOT NULL,
	bank_swift_code text NOT NULL,
	bank_account_name text NOT NULL,
	bank_account_number text NOT NULL,
	PRIMARY KEY (customer_id), 
	FOREIGN KEY (customer_id) ACCESSED_BY customer(customer_id)
);

CREATE TABLE customer_affiliate_report (
	customer_affiliate_id int NOT NULL,
	customer_id int NOT NULL,
	tracking text NOT NULL,
	amount int NOT NULL,
	description text NOT NULL,
	date_added datetime NOT NULL,
	PRIMARY KEY (customer_affiliate_id),
	FOREIGN KEY (customer_id) REFERENCES customer(customer_id)
);

CREATE TABLE customer_approval (
	customer_approval_id int NOT NULL,
	customer_id int NOT NULL,
	PRIMARY KEY (customer_approval_id),
	FOREIGN KEY (customer_id) REFERENCES customer(customer_id)
);

CREATE TABLE customer_group (
	customer_group_id int NOT NULL,
	approval int NOT NULL,
	sort_order int NOT NULL,
	PRIMARY KEY (customer_group_id)
);

CREATE TABLE customer_history (
	customer_history_id int NOT NULL,
	customer_id int NOT NULL,
	comment text NOT NULL,
	date_added datetime NOT NULL,
	PRIMARY KEY (customer_history_id),
	FOREIGN KEY (customer_id) OWNED_BY customer(customer_id)
);

-- ? should this be a data_subject? No foreign key to customer(customer_id)
CREATE TABLE customer_login (
	customer_login_id int NOT NULL,
	email text NOT NULL,
	ip text NOT NULL,
	total int NOT NULL,
	date_added datetime NOT NULL,
	date_modified datetime NOT NULL,
	PRIMARY KEY (customer_login_id)
);

CREATE TABLE customer_ip (
	customer_ip_id int NOT NULL,
	customer_id int NOT NULL,
	ip text NOT NULL,
	date_added datetime NOT NULL,
	PRIMARY KEY (customer_ip_id),
	FOREIGN KEY (customer_id) OWNED_BY customer(customer_id)
);

CREATE TABLE customer_online (
	customer_online_id int NOT NULL,
	customer_id int NOT NULL,
	ip text NOT NULL,
	url text NOT NULL,
	referer text NOT NULL,
	date_added datetime NOT NULL,
	PRIMARY KEY (customer_online_id),
	FOREIGN KEY (customer_id) OWNED_BY customer(customer_id)
);

CREATE TABLE customer_reward (
	customer_reward_id int NOT NULL,
	customer_id int NOT NULL,
	order_id int NOT NULL,
	description text NOT NULL,
	points int NOT NULL,
	date_added datetime NOT NULL,
	PRIMARY KEY (customer_reward_id),
	FOREIGN KEY (customer_id) REFERENCES customer(customer_id)
);

CREATE TABLE customer_transaction (
	customer_transaction_id int NOT NULL,
	customer_id int NOT NULL,
	order_id int NOT NULL,
	description text NOT NULL,
	amount int NOT NULL,
	date_added datetime NOT NULL,
	PRIMARY KEY (customer_transaction_id),
	FOREIGN KEY (customer_id) OWNED_BY customer(customer_id)
);

CREATE TABLE customer_search (
	customer_search_id int NOT NULL,
	store_id int NOT NULL,
	language_id int NOT NULL,
	customer_id int NOT NULL,
	keyword text NOT NULL,
	category_id int NOT NULL,
	sub_category int NOT NULL,
	description int NOT NULL,
	products int NOT NULL,
	ip text NOT NULL,
	date_added datetime NOT NULL,
	PRIMARY KEY (customer_search_id),
	FOREIGN KEY (customer_id) OWNED_BY customer(customer_id)
);

CREATE TABLE customer_wishlist (
	customer_id int NOT NULL,
	product_id int NOT NULL,
	date_added datetime NOT NULL,
	PRIMARY KEY (customer_id),
	FOREIGN KEY (customer_id) REFERENCES customer(customer_id)
);

CREATE TABLE custom_field (
	custom_field_id int NOT NULL,
	type text NOT NULL,
	value text NOT NULL,
	location text NOT NULL,
	status int NOT NULL,
	sort_order int NOT NULL,
	PRIMARY KEY (custom_field_id)
);

CREATE TABLE custom_field_customer_group (
	custom_field_id int NOT NULL,
	customer_group_id int NOT NULL,
	PRIMARY KEY (custom_field_id)
);

CREATE TABLE custom_field_description (
	custom_field_id int NOT NULL,
	language_id int NOT NULL,
	name text NOT NULL,
	PRIMARY KEY (custom_field_id)
);

CREATE TABLE custom_field_value (
	custom_field_value_id int NOT NULL,
	custom_field_id int NOT NULL,
	sort_order int NOT NULL,
	PRIMARY KEY (custom_field_value_id)
);

CREATE TABLE download (
	download_id int NOT NULL,
	filename text NOT NULL,
	mask text NOT NULL,
	date_added datetime NOT NULL,
	PRIMARY KEY (download_id)
);

CREATE TABLE gdpr (
	gdpr_id int NOT NULL,
	name text NOT NULL,
	description text NOT NULL,
	status int NOT NULL,
	PRIMARY KEY (gdpr_id)
);

CREATE TABLE geo_zone (
	geo_zone_id int NOT NULL,
	name text NOT NULL,
	description text NOT NULL,
	date_modified datetime NOT NULL,
	PRIMARY KEY (geo_zone_id)
);

CREATE TABLE location (
	location_id int NOT NULL,
	name text NOT NULL,
	address text NOT NULL,
	telephone text NOT NULL,
	fax text NOT NULL,
	geocode text NOT NULL,
	image text NOT NULL,
	open text NOT NULL,
	comment text NOT NULL,
	PRIMARY KEY (location_id)
);

CREATE TABLE order1 (
	order_id int NOT NULL,
	invoice_no int NOT NULL,
	invoice_prefix text NOT NULL,
	store_id int NOT NULL,
	store_name text NOT NULL,
	store_url text NOT NULL,
	customer_id int NOT NULL,
	customer_group_id int NOT NULL,
	firstname text NOT NULL,
	lastname text NOT NULL,
	email text NOT NULL,
	telephone text NOT NULL,
	fax text NOT NULL,
	custom_field text NOT NULL,
	payment_firstname text NOT NULL,
	payment_lastname text NOT NULL,
	payment_company text NOT NULL,
	payment_address_1 text NOT NULL,
	payment_address_2 text NOT NULL,
	payment_city text NOT NULL,
	payment_postcode text NOT NULL,
	payment_country text NOT NULL,
	payment_country_id int NOT NULL,
	payment_zone text NOT NULL,
	payment_zone_id int NOT NULL,
	payment_address_format text NOT NULL,
	payment_custom_field text NOT NULL,
	payment_method text NOT NULL,
	payment_code text NOT NULL,
	shipping_firstname text NOT NULL,
	shipping_lastname text NOT NULL,
	shipping_company text NOT NULL,
	shipping_address_1 text NOT NULL,
	shipping_address_2 text NOT NULL,
	shipping_city text NOT NULL,
	shipping_postcode text NOT NULL,
	shipping_country text NOT NULL,
	shipping_country_id int NOT NULL,
	shipping_zone text NOT NULL,
	shipping_zone_id int NOT NULL,
	shipping_address_format text NOT NULL,
	shipping_custom_field text NOT NULL,
	shipping_method text NOT NULL,
	shipping_code text NOT NULL,
	comment text NOT NULL,
	total int NOT NULL,
	order_status_id int NOT NULL,
	affiliate_id int NOT NULL,
	commission int NOT NULL,
	marketing_id int NOT NULL,
	tracking text NOT NULL,
	language_id int NOT NULL,
	currency_id int NOT NULL,
	currency_code text NOT NULL,
	currency_value int NOT NULL,
	ip text NOT NULL,
	forwarded_ip text NOT NULL,
	user_agent text NOT NULL,
	accept_language text NOT NULL,
	date_added datetime NOT NULL,
	date_modified datetime NOT NULL,
	PRIMARY KEY (order_id),
	FOREIGN KEY (customer_id) OWNED_BY customer(customer_id)
);

CREATE TABLE order_history (
	order_history_id int NOT NULL,
	order_id int NOT NULL,
	order_status_id int NOT NULL,
	notify int NOT NULL,
	comment text NOT NULL,
	date_added datetime NOT NULL,
	PRIMARY KEY (order_history_id)
);

CREATE TABLE order_product (
	order_product_id int NOT NULL,
	order_id int NOT NULL,
	product_id int NOT NULL,
	name text NOT NULL,
	model text NOT NULL,
	quantity int NOT NULL,
	price int NOT NULL,
	total int NOT NULL,
	tax int NOT NULL,
	reward int NOT NULL,
	PRIMARY KEY (order_product_id)
);

CREATE TABLE order_recurring (
	order_recurring_id int NOT NULL,
	order_id int NOT NULL,
	recurring_id int NOT NULL,
	reference text NOT NULL,
	products text NOT NULL,
	product_name text NOT NULL,
	product_quantity int NOT NULL,
	recurring_name text NOT NULL,
	recurring_description text NOT NULL,
	recurring_frequency text NOT NULL,
	recurring_cycle int NOT NULL,
	recurring_duration int NOT NULL,
	recurring_price int NOT NULL,
	trial text NOT NULL,
	trial_frequency text NOT NULL,
	trial_cycle int NOT NULL,
	trial_duration int NOT NULL,
	trial_price int NOT NULL,
	status int NOT NULL,
	date_added datetime NOT NULL,
	PRIMARY KEY (order_recurring_id)
);

CREATE TABLE order_recurring_transaction (
	order_recurring_transaction_id int NOT NULL,
	order_recurring_id int NOT NULL,
	reference text NOT NULL,
	type text NOT NULL,
	amount int NOT NULL,
	date_added datetime NOT NULL,
	PRIMARY KEY (order_recurring_transaction_id)
);

CREATE TABLE order_shipment (
	order_shipment_id int NOT NULL,
	order_id int NOT NULL,
	date_added datetime NOT NULL,
	PRIMARY KEY (order_shipment_id)
);

CREATE TABLE return (
	return_id int NOT NULL,
	order_id int NOT NULL,
	product_id int NOT NULL,
	customer_id int NOT NULL,
	firstname text NOT NULL,
	lastname text NOT NULL,
	email text NOT NULL,
	telephone text NOT NULL,
	product text NOT NULL,
	model text NOT NULL,
	quantity int NOT NULL,
	opened int NOT NULL,
	return_reason_id int NOT NULL,
	return_action_id int NOT NULL,
	return_status_id int NOT NULL,
	comment text NOT NULL,
	date_ordered datetime NOT NULL,
	date_added datetime NOT NULL,
	date_modified datetime NOT NULL,
	PRIMARY KEY (return_id),
	FOREIGN KEY (customer_id) REFERENCES customer (customer_id)
);

CREATE TABLE review (
	review_id int NOT NULL,
	product_id int NOT NULL,
	customer_id int NOT NULL,
	author text NOT NULL,
	text text NOT NULL,
	rating int NOT NULL,
	status int NOT NULL,
	date_added datetime NOT NULL,
	date_modified datetime NOT NULL,
	PRIMARY KEY (review_id),
	FOREIGN KEY (customer_id) REFERENCES customer (customer_id)
);

CREATE TABLE tax_rate_to_customer_group (
	tax_rate_id int NOT NULL,
	customer_group_id int NOT NULL,
	PRIMARY KEY (tax_rate_id)
);

CREATE TABLE user_group (
	user_group_id int NOT NULL,
	name text NOT NULL,
	permission text NOT NULL,
	PRIMARY KEY (user_group_id)
);

EXPLAIN COMPLIANCE;