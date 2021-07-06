--start frontpage
SELECT * FROM q35
SELECT * FROM q18 WHERE user_id = 1 
SELECT * FROM q21 WHERE user_id = 1 
SELECT * FROM q37 WHERE id IN (0, 1, 2)
SELECT * FROM q25 WHERE story_id IN (1, 2, 4) 
SELECT * FROM q28 WHERE story_id IN (1, 2, 4) 
SELECT * FROM q26 WHERE story_id IN (1, 2, 4) 
SELECT * FROM q29 WHERE id IN (1) 
SELECT * FROM q5 WHERE OWNER_user_id = 2 AND story_id IN (1, 2) 
SELECT * FROM q20 WHERE story_id IN (1, 2, 4) AND user_id = 2
SELECT * FROM q24 WHERE story_id IN (1, 2, 4) AND user_id = 2
--end frontpage
