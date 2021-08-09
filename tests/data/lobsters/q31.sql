# TODO(babman): the `one` thing again + story_id and user_id filtered but not projected again.
# CREATE VIEW q31 AS '"SELECT 1 FROM hidden_stories WHERE user_id = 0 AND hidden_stories.story_id = 0"';
SELECT * FROM q31 WHERE user_id = 0 AND story_id = 0;
