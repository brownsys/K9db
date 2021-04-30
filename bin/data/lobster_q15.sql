CREATE VIEW q15 AS '"SELECT read_ribbons.id, read_ribbons.is_following, read_ribbons.created_at, read_ribbons.updated_at, read_ribbons.user_id, read_ribbons.story_id FROM read_ribbons WHERE read_ribbons.user_id = 0 AND read_ribbons.story_id = 0"';
SELECT * FROM q15;
