INSERT INTO shuup_contact(id, PII_name) VALUES (1, 'Alice');
INSERT INTO shuup_contact(id, PII_name) VALUES (2, 'Banji');
INSERT INTO shuup_contact(id, PII_name) VALUES (3, 'Keenan');

INSERT INTO auth_user VALUES (10, 'Alice', 'secure');
INSERT INTO auth_user VALUES (20, 'Banji', 'password');

INSERT INTO shuup_personcontact VALUES (1, 'Alice', 10, "email@email.com");
INSERT INTO shuup_personcontact VALUES (2, 'Banji', 20, "banj@gmail.com");
INSERT INTO shuup_personcontact VALUES (3, 'Keenan', 0, "keenan@hotmail.com");
