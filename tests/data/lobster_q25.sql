CREATE VIEW q25 AS '"SELECT suggested_titles.id, suggested_titles.story_id, suggested_titles.user_id, suggested_titles.title FROM suggested_titles WHERE suggested_titles.story_id = 0"';
SELECT * FROM q25;
