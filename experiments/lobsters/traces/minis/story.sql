--start story
SELECT * FROM q7 WHERE short_id = '00000d' 
SELECT * FROM q37 WHERE id = 1 
SELECT * FROM q15 WHERE story_id = 3 AND user_id = 1 
INSERT INTO read_ribbons (id, created_at, updated_at, user_id, story_id, is_following) VALUES (22, '2021-05-07 06:57:37.154741471', '2021-05-07 06:57:37.154741471', 5767, 33229, 1)
SELECT * FROM q11 WHERE merged_story_id = NULL 
SELECT * FROM q12 WHERE story_id = 4 
SELECT * FROM q37 WHERE id IN (1, 3, 4) 
SELECT * FROM q17 WHERE comment_id IN (1, 3, 4) 
SELECT * FROM q5 WHERE OWNER_user_id = 3 AND story_id = 1 
SELECT * FROM q20 WHERE story_id = 3 AND user_id = 1 
SELECT * FROM q24 WHERE story_id = 3 AND user_id = 1 
SELECT * FROM q26 WHERE story_id = 3 
SELECT * FROM q29 WHERE id IN (1) 
--end: story
