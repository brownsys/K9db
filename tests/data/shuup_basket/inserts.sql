INSERT INTO shuup_personcontact VALUES (1, 100, 'Alice', 10, "email@email.com");
INSERT INTO shuup_personcontact VALUES (2, 200, 'Banji', 20, "banj@gmail.com");
INSERT INTO shuup_personcontact VALUES (3, 300, 'Keenan', 0, "keenan@hotmail.com");

INSERT INTO shuup_contact(id, ptr, name) VALUES (6, 100, 'Alice');
INSERT INTO shuup_contact(id, name) VALUES (7, 200, 'Banji');
INSERT INTO shuup_contact(id, name) VALUES (8, 300, 'Keenan');

INSERT INTO auth_user(id, ptr, username, password) VALUES (15, 10, 'Alice', 'secure');
INSERT INTO auth_user VALUES (16, 20, 'Banji', 'password');

INSERT INTO shuup_order VALUES (1, 1, 5, 'reference_number', '401-401-4010', 'banjy@evil.com', 0, 1, 0, 'cash', 100, 'USD', '2021-04-21 01:00:00', '2021-04-21 01:00:00', 1, 1);
