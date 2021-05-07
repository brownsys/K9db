--start story
SELECT * FROM q7 WHERE short_id = '000pmu' 
SELECT * FROM q37 WHERE id = 5795 
SELECT * FROM q15 WHERE story_id = 33229 AND user_id = 5767 
INSERT INTO read_ribbons (id, created_at, updated_at, user_id, story_id, is_following) VALUES (22, '2021-05-07 06:57:37.154741471', '2021-05-07 06:57:37.154741471', 5767, 33229, 1)
SELECT * FROM q11 WHERE merged_story_id = 33229 
SELECT * FROM q12 WHERE story_id = 33229 
SELECT * FROM q37 WHERE id IN (5718, 5675, 3966) 
SELECT * FROM q17 WHERE comment_id IN (73865, 114533, 33221) 
SELECT * FROM q5 WHERE OWNER_user_id = 5767 AND story_id = 33229 
SELECT * FROM q20 WHERE story_id = 33229 AND user_id = 5767 
SELECT * FROM q24 WHERE story_id = 33229 AND user_id = 5767 
SELECT * FROM q26 WHERE story_id = 33229 
SELECT * FROM q29 WHERE id IN (1) 
--end: story
