--start: comment_vote
SELECT * FROM q30
SELECT * FROM q10 WHERE OWNER_user_id = 2 AND comment_id = 1 AND story_id = 1
INSERT INTO votes (id, OWNER_user_id, story_id, comment_id, vote, reason) VALUES (161923, 5, 1, NULL, 1, NULL)
UPDATE users SET users.karma = users.karma + 1 WHERE users.id = 3
UPDATE comments SET comments.upvotes = comments.upvotes + 1, comments.downvotes = comments.downvotes + 0, comments.confidence = 1 WHERE id = 2
SELECT * FROM q35
SELECT * FROM q13 WHERE story_id = 2
SELECT * FROM q6 WHERE story_id = 2 
SELECT * FROM q11 WHERE merged_story_id = NULL 
UPDATE stories SET stories.upvotes = stories.upvotes + 1, stories.downvotes = stories.downvotes + 0, stories.hotness = 19213 WHERE id = 3
--end: comment_vote
