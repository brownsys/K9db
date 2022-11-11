INSERT INTO user VALUES (10, 'first_user');
INSERT INTO user VALUES (11, 'second_user');
INSERT INTO rel (to_t, id, from_t) VALUES (2000, 200, 10);
INSERT INTO t values (2000, 'target');
INSERT INTO rel (to_t, id, from_t) VALUES (2000, 200, 11);

#GDPR GET user 10;
#GDPR GET user 11;
