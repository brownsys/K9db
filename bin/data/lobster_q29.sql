CREATE VIEW replying_comments_for_count AS '"SELECT read_ribbons.user_id, read_ribbons.story_id, comments.id, COUNT(*) FROM read_ribbons JOIN stories ON (read_ribbons.story_id = stories.id) JOIN comments ON (read_ribbons.story_id = comments.story_id) LEFT JOIN comments AS parent_comments ON (comments.parent_comment_id = parent_comments.id) WHERE read_ribbons.is_following = 1 AND comments.user_id <> read_ribbons.user_id AND comments.is_deleted = 0 AND comments.is_moderated = 0 AND ( comments.upvotes - comments.downvotes ) >= 0 AND read_ribbons.updated_at < comments.created_at AND ( ( parent_comments.user_id = read_ribbons.user_id AND ( parent_comments.upvotes - parent_comments.downvotes ) >= 0 ) OR ( parent_comments.id IS NULL AND stories.user_id = read_ribbons.user_id)) AND read_ribbons.user_id = 0 GROUP BY read_ribbons.user_id, read_ribbons.story_id, comments.id"';
SELECT * FROM q29;