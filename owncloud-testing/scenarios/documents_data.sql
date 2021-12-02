INSERT INTO oc_users (uid, PII_displayname, password) VALUES ('claire', 'Claire', 'password');
INSERT INTO oc_users (uid, PII_displayname, password) VALUES ('wong', 'Wong', 'password');

INSERT INTO oc_share 
    (id, share_type, OWNER_share_with, uid_owner, uid_initiator, parent, item_type, item_source, item_target, file_target, permissions, stime, accepted, expiration, token, mail_send, share_name, file_source, attributes)
VALUES (0, 0, 'wong', 'claire', 'claire', NULL, 'file', '/books/The Sorcerers Stone.ebook', '', '', 24, 0, 0, NULL, '', 1, '', 24, 19);