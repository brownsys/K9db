CREATE VIEW q2 AS '"SELECT 1 AS `one` FROM stories WHERE stories.short_id = 'joe'"';
SELECT * FROM q2;