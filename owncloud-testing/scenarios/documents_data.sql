INSERT INTO oc_users (uid, PII_displayname, password) VALUES ('claire', 'Claire', 'password');
INSERT INTO oc_users (uid, PII_displayname, password) VALUES ('wong', 'Wong', 'password');


INSERT INTO oc_share 
    --(id, share_type, OWNER_share_with, uid_owner, uid_initiator, parent, item_type, OWNING_item_source, item_target, file_target, permissions, stime, accepted, expiration, token, mail_send, share_name, file_source, attributes)
VALUES (0, 0, 'wong', 'claire', 'claire', NULL, 'file', 0, '', '', 24, 0, 0, NULL, '', 1, '', 24, 19);
INSERT INTO oc_share 
VALUES (1, 0, 'claire', 'wong', 'wong', NULL, 'file', 1, '', '', 24, 0, 0, NULL, '', 1, '', 24, 19);

INSERT INTO oc_files (id, file_name) VALUES (0, '/books/The Sorcerers Stone.ebook');
INSERT INTO oc_files (id, file_name) VALUES (1, '/books/The Chamber of Secrets.ebook');