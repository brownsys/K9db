SELECT * FROM q5 WHERE votes.OWNER_user_id = 0 AND votes.story_id = 0;
SELECT * FROM votes WHERE OWNER_user_id = 0 AND story_id = 0 AND comment_id IS NULL;
