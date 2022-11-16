EXPLAIN SELECT * FROM votes WHERE user_id = ? AND story_id = ? AND comment_id IS NULL;
EXPLAIN SELECT 1 AS `one` FROM stories WHERE stories.short_id = ?;
EXPLAIN SELECT * FROM votes WHERE user_id = ? AND comment_id = ?;
EXPLAIN SELECT * FROM votes WHERE user_id = ? AND story_id = ?;
