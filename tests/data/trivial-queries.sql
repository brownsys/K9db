CREATE VIEW comments_view AS '"SELECT id FROM comments"';

INSERT INTO users VALUES (1, 'admin', 1);
INSERT INTO users VALUES (2, 'joe', 0);

INSERT INTO comments VALUES (1, '2021-04-21 01:00:00', NULL, 'asdf', 1, 2, NULL, NULL, 'yo', 0, 0, 0, 0, 0, 0, 0, 0);
INSERT INTO comments VALUES (2, '2021-04-21 01:00:00', NULL, 'asdf2', 2, 1, NULL, NULL, 'yo2', 0, 0, 0, 0, 0, 0, 0, 0);

SELECT * FROM comments_view;
