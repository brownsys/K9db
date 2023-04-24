SELECT * FROM q30;
SELECT comments.* FROM comments WHERE comments.is_deleted = 0 AND comments.is_moderated = 0 ORDER BY id LIMIT 40;
