INSERT INTO user VALUES (5, 'test');

INSERT INTO rel VALUES (105, 5, 1005);

INSERT INTO t VALUES (1005, 'target_del');

DELETE FROM rel WHERE id = 105;

GDPR GET user 5;
