SELECT * FROM q35;
SELECT stories.*, upvotes - downvotes AS saldo FROM stories WHERE stories.merged_story_id IS NULL AND stories.is_expired = 0 ORDER BY id DESC LIMIT 51;
