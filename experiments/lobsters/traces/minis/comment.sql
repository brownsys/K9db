--start: comment
SELECT * FROM q7 WHERE short_id = '00000d' 
SELECT * FROM q37 WHERE id = 1 
SELECT * FROM q9 WHERE short_id = '000008' 
INSERT INTO comments (id, created_at, updated_at, short_id, story_id, user_id, parent_comment_id, thread_id, comment, upvotes, confidence, markeddown_comment,downvotes, is_deleted, is_moderated, is_from_email, hat_id) VALUES (121275, '2021-05-07 07:34:22.752279579', '2021-05-07 07:34:22.752279579', '3lwk5s', 7029, 3, NULL, NULL, 'moar benchmarking', 1, 1, '<p>moar benchmarking</p>', 0, 0, 0, 0, NULL)
SELECT * FROM q10 WHERE OWNER_user_id = 0 AND comment_id = 3 AND story_id = 4
INSERT INTO votes (id, OWNER_user_id, story_id, comment_id, vote, reason) VALUES (161934, 2, 7029, 121275, 1, NULL)
SELECT * FROM q11 WHERE merged_story_id = NULL 
SELECT * FROM q30
UPDATE stories SET comments_count = 4 WHERE stories.id = 1
SELECT * FROM q13 WHERE story_id = 1 
SELECT * FROM q6 WHERE story_id = 1 
SELECT * FROM q11 WHERE merged_story_id = NULL 
UPDATE stories SET hotness = 19213 WHERE stories.id = 1
REPLACE INTO keystores (keyX, valueX) VALUES ('user:2270:comments_posted', 1)
--end: comment
