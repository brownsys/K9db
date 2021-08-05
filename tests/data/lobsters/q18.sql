# TODO(babman): similar thing with user_id.
#CREATE VIEW q18 AS '"SELECT hidden_stories.story_id FROM hidden_stories WHERE hidden_stories.user_id = 0"';
SELECT * FROM q18 WHERE user_id = 0;
