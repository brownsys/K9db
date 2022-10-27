# TODO(babman): the `one` thing again + story_id and OWNER_user_id filtered but not projected again.
# CREATE VIEW q31 AS '"SELECT 1 FROM hidden_stories WHERE OWNER_user_id = 0 AND hidden_stories.story_id = 0"';
SELECT * FROM q31 WHERE OWNER_user_id = 0 AND story_id = 0;
SELECT 1 AS `one`, OWNER_user_id, story_id FROM hidden_stories WHERE OWNER_user_id = 0 AND story_id = 0;
