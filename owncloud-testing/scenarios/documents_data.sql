CREATE TABLE username_marker (uid TEXT REFERENCES oc_users(uid), uname TEXT);


INSERT INTO oc_users (uid, PII_displayname, password) VALUES ('claire', 'Claire', 'password');
INSERT INTO oc_users (uid, PII_displayname, password) VALUES ('wong', 'Wong', 'password');

INSERT INTO username_marker VALUES ('claire', 'claire');
INSERT INTO username_marker VALUES ('wong', 'wong');


INSERT INTO oc_share 
    --(id, share_type, OWNER_share_with, OWNER_share_with_group, uid_owner, uid_initiator, parent, item_type, OWNING_item_source, item_target, file_target, permissions, stime, accepted, expiration, token, mail_send, share_name, file_source, attributes)
VALUES (0, 0, 'wong', NULL, 'claire', 'claire', NULL, 'file', 0, '', '', 24, 0, 0, NULL, '', 1, '', 24, 19);
INSERT INTO oc_share 
VALUES (1, 0, 'claire', NULL, 'wong', 'wong', NULL, 'file', 1, '', '', 24, 0, 0, NULL, '', 1, '', 24, 19);

INSERT INTO oc_files (id, file_name) VALUES (0, '/books/The Sorcerers Stone.ebook');
INSERT INTO oc_files (id, file_name) VALUES (1, '/books/The Chamber of Secrets.ebook');

INSERT INTO oc_groups VALUES ('group_1');
INSERT INTO oc_group_user VALUES (0, 'group_1', 'wong');
INSERT INTO oc_group_user VALUES (1, 'group_1', 'claire');

INSERT INTO oc_users VALUES ('kashvi', 'Kashvi', 'password');
INSERT INTO username_marker VALUES ('kashvi', 'kashvi');

INSERT INTO oc_share VALUES (2, 1, NULL, 'group_1', 'kashvi', 'kashvi', NULL, 'file', 2, '', '', 24, 0, 0, NULL, '', 1, '', 24, 19);

INSERT INTO oc_files VALUES (2, '/books/The Prisoner of Azkaban.ebook');
