SELECT * FROM q12 WHERE story_id = 0;
SELECT comments.*, comments.upvotes - comments.downvotes AS saldo FROM comments WHERE comments.story_id = 0;
