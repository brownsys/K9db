CREATE TABLE username_marker (uid VARCHAR(64) REFERENCES oc_users(uid), uname TEXT, id INT PRIMARY KEY);


INSERT INTO oc_users (uid, PII_displayname, password) VALUES ('claire', 'Claire', 'hp_good_jkr_bad');
INSERT INTO oc_users (uid, PII_displayname, password) VALUES ('wong', 'Wong', 'hp_good_jkr_bad');

INSERT INTO username_marker VALUES ('claire', 'claire', 1);
INSERT INTO username_marker VALUES ('wong', 'wong', 2);


-- INSERT INTO oc_share 
-- VALUES (0, 0, 'wong', NULL, 'claire', 'claire', NULL, 'file', 0, '', '', 24, 0, 0, NULL, '', 1, '', 24, 19);
INSERT INTO oc_share 
VALUES (0, 0, 'wong', NULL, 'claire', 'claire', NULL, 'file', 0, '', '', 24, 0, 0, NULL, '', 1, '', 24, 19);

INSERT INTO oc_files (id, file_name) VALUES (0, '/books/The Sorcerers Stone.ebook');

INSERT INTO oc_group_user VALUES (0, 'HP Fans', 'wong');
INSERT INTO oc_group_user VALUES (1, 'HP Fans', 'claire');
INSERT INTO oc_groups VALUES ('HP Fans');

INSERT INTO oc_users VALUES ('kashvi', 'Kashvi', 'password');
INSERT INTO username_marker VALUES ('kashvi', 'kashvi', 3);

INSERT INTO oc_share VALUES (2, 1, NULL, 'HP Fans', 'kashvi', 'kashvi', NULL, 'file', 2, '', '', 24, 0, 0, NULL, '', 1, '', 24, 19);

INSERT INTO oc_files VALUES (2, '/books/The Prisoner of Azkaban.ebook');
