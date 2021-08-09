# TODO(babman): check that planning something like this works.
# CREATE VIEW q2 AS '"SELECT 1 AS `one` FROM stories WHERE stories.short_id = ?"';
SELECT * FROM q2 WHERE short_id = 'joe';
SELECT 1 AS `one`, short_id FROM stories WHERE short_id = 'joe';
