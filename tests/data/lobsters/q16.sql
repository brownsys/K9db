SELECT * FROM q16;
SELECT stories.* FROM stories WHERE stories.merged_story_id IS NULL AND stories.is_expired = 0 AND stories.upvotes - stories.downvotes >= 0 ORDER BY hotness ASC LIMIT 51;
