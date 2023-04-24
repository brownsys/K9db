SELECT * FROM q6 WHERE story_id = 0;
SELECT comments.upvotes, comments.downvotes, comments.story_id FROM comments JOIN stories ON comments.story_id = stories.id WHERE comments.story_id = 0 AND comments.user_id != stories.user_id;
