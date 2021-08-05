# TODO(babman): check that planning something like this works.
# CREATE VIEW q9 AS '"SELECT 1 AS `one` FROM comments WHERE comments.short_id = 'comment1'"';
SELECT * FROM q9 WHERE short_id = 'comment1';
