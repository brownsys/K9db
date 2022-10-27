# TODO(babman): similar thing with OWNER_user_id getting projected out.
#CREATE VIEW q18 AS '"SELECT hidden_stories.story_id FROM hidden_stories WHERE hidden_stories.OWNER_user_id = 0"';
SELECT * FROM q18 WHERE OWNER_user_id = 0;
SELECT story_id, OWNER_user_id FROM hidden_stories WHERE OWNER_user_id = 0;
