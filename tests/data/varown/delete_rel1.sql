INSERT INTO user VALUES (5, 'test');

INSERT INTO t VALUES (1005, 'target_del');

INSERT INTO rel VALUES (105, 5, 1005);

DELETE FROM rel WHERE id = 105;

GDPR GET user 5;
