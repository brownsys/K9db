CREATE VIEW q6 AS '"SELECT comments.upvotes, comments.downvotes FROM comments JOIN stories ON comments.story_id = stories.id WHERE comments.story_id = 0 AND comments.user_id != stories.user_id"';
SELECT * FROM q6;
