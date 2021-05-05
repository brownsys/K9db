CREATE VIEW q1 AS '"SELECT 1 AS `one` FROM users WHERE users.PII_username = 'joe'"';
SELECT * FROM q1;