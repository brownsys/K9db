# TODO(babman): check that planning something like this works.
# CREATE VIEW q1 AS '"SELECT 1 AS `one` FROM users WHERE users.PII_username = ?"';
SELECT * FROM q1 WHERE PII_username = 'joe';
SELECT 1 AS `one`, PII_username FROM users WHERE PII_username = 'joe';
