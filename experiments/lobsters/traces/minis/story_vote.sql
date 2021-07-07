--start: story_vote
SELECT * FROM q7 WHERE short_id = '000fpi' 
SELECT * FROM q5 WHERE OWNER_user_id = 5619 AND story_id = 20360 
INSERT INTO votes (id, OWNER_user_id, story_id, comment_id, vote, reason) VALUES (161921, 5619, 20360, NULL, 1, NULL)
UPDATE users SET users.karma = users.karma + 1 WHERE users.id = 5784
SELECT * FROM q13 WHERE story_id = 20360 
SELECT * FROM q6 WHERE story_id = 20360 
SELECT * FROM q11 WHERE merged_story_id = 20360 
UPDATE stories SET stories.upvotes = stories.upvotes + 1, stories.downvotes = stories.downvotes + 0, stories.hotness = 19213 WHERE stories.id = 20360
--end: story_vote
