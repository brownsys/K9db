INSERT INTO user VALUES (8, 'test');
INSERT INTO user VALUES (9, 'b');

INSERT INTO t VALUES (1008, 'target_del_2');

INSERT INTO rel VALUES (108, 8, 1008);
INSERT INTO rel VALUES (109, 9, 1008);

DELETE FROM rel WHERE id = 108;

GDPR GET user 8;
GDPR GET user 9;
