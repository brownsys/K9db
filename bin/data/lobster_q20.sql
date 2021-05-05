CREATE VIEW q20 AS '"SELECT hidden_stories.id, hidden_stories.user_id, hidden_stories.story_id FROM hidden_stories WHERE hidden_stories.user_id = 0 AND hidden_stories.story_id = 0"';
SELECT * FROM q20;
