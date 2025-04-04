CTX START;
INSERT INTO shuup_contact(id, name, email) VALUES (6, 'Alice', 'alice@email.com');
INSERT INTO auth_user(id, username, password) VALUES (15, 'Alice', 'secure');
INSERT INTO shuup_personcontact(pid, contact_ptr_id, name, user_id, email) VALUES (1, 6, 'Alice', 15, 'alice@email.com');
CTX COMMIT;

CTX START;
INSERT INTO shuup_contact(id, name, email) VALUES (7, 'Joe', 'joe@email.com');
INSERT INTO auth_user(id, username, password) VALUES (16, 'Joe', 'insecure');
INSERT INTO shuup_personcontact(pid, contact_ptr_id, name, user_id, email) VALUES (2, 7, 'Joe', 16, 'joe@email.com');
CTX COMMIT;

CTX START;
INSERT INTO shuup_contact(id, name, email) VALUES (8, 'Company A', 'company@email.com');
INSERT INTO shuup_companycontact(contact_ptr_id, tax_number) VALUES (8, 666);
INSERT INTO shuup_companycontact_members(id, companycontact_id, contact_id) VALUES (1, 8, 1);
CTX ROLLBACK;


INSERT INTO shuup_shop(id, owner_id) VALUES (1, 1);
INSERT INTO shuup_gdpr_gdpruserconsent(id, created_on, shop_id, user_id) VALUES (1, '2020-12-30 00:00:00', 1, 15);

CTX START;
INSERT INTO shuup_mutaddress(id, tax_number, user_id) VALUES (71, 6666, 15);
INSERT INTO shuup_order(id, creator_id, customer_id, modified_by_id, shop_id, orderer_id, account_manager_id, billing_address_id) VALUES (1, 15, 6, 15, 1, 1, 2, 71);
CTX COMMIT;

INSERT INTO shuup_basket(id, creator_id, customer_id, orderer_id, shop_id) VALUES (1, 15, 1, 2, 1);
