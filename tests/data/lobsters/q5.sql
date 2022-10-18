SELECT * FROM q5 WHERE votes.user_id = 0 AND votes.story_id = 0;
SELECT * FROM votes WHERE user_id = 0 AND story_id = 0 AND comment_id IS NULL;
