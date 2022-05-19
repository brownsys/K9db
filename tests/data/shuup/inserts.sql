INSERT INTO shuup_personcontact(pid, OWNING_contact_ptr_id, PII_name, OWNING_user_id, email) VALUES (1, 100, 'Alice', 10, 'email@email.com');
INSERT INTO shuup_contact(id, ptr, name) VALUES (6, 100, 'Alice');
INSERT INTO auth_user(id, ptr, username, password) VALUES (15, 10, 'Alice', 'secure');

INSERT INTO shuup_personcontact(pid, OWNING_contact_ptr_id, PII_name, OWNING_user_id, email) VALUES (2, 200, 'Joe', 20, 'email@email.com');
INSERT INTO shuup_contact(id, ptr, name) VALUES (7, 200, 'Joe');
INSERT INTO auth_user(id, ptr, username, password) VALUES (16, 20, 'Joe', 'insecure');

INSERT INTO shuup_companycontact(contact_ptr_id, tax_number) VALUES (100, 6666);
INSERT INTO shuup_companycontact_members(id, companycontact_id, ACCESSOR_contact_id) VALUES (1, 100, 1);

INSERT INTO shuup_shop(id, OWNER_owner_id) VALUES (1, 15);
INSERT INTO shuup_gdpr_gdpruserconsent(id, created_on, shop_id, OWNER_user_id) VALUES (1, "00:00:00", 1, 15);

INSERT INTO shuup_mutaddress(id, tax_number, ACCESSOR_ANONYMIZE_user_id) VALUES (1, 6666, 15);

INSERT INTO shuup_basket(id, creator_id, OWNER_customer_id, ACCESSOR_ANONYMIZE_orderer_id, shop_id) VALUES (1, 15, 1, 2, 1);

INSERT INTO shuup_order(id, OWNER_creator_id, OWNER_customer_id, ACCESSOR_modified_by_id, ACCESSOR_shop_id, ACCESSOR_ANONYMIZE_orderer_id, account_manager_id, OWNS_billing_address_id) VALUES (1, 15, 6, 15, 1, 1, 2, 666);