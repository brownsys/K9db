# TODO(babman): check that planning something like this works.
# CREATE VIEW q1 AS '"SELECT 1 AS `one` FROM users WHERE users.username = ?"';
SELECT * FROM q1 WHERE username = 'joe';
SELECT 1 AS `one`, username FROM users WHERE username = 'joe';
