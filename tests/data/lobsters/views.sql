CREATE VIEW q6 AS '"SELECT comments.upvotes, comments.downvotes, comments.story_id FROM comments JOIN stories ON comments.story_id = stories.id WHERE comments.story_id = ? AND comments.user_id != stories.user_id"';
CREATE VIEW q12 AS '"SELECT comments.*, comments.upvotes - comments.downvotes AS saldo FROM comments WHERE comments.story_id = ? ORDER BY saldo ASC, confidence DESC"';
CREATE VIEW q13 AS '"SELECT tags.*, taggings.story_id FROM tags INNER JOIN taggings ON tags.id = taggings.tag_id WHERE taggings.story_id = ?"';
-- rewrote net_votes as arith expr in the WHERE clause
CREATE VIEW q16 AS '"SELECT stories.* FROM stories WHERE stories.merged_story_id IS NULL AND stories.is_expired = 0 AND stories.upvotes - stories.downvotes >= 0 ORDER BY hotness ASC LIMIT 51"';
CREATE VIEW q22 AS '"SELECT tags.id, stories.user_id, count(*) AS `count` FROM taggings INNER JOIN tags ON taggings.tag_id = tags.id INNER JOIN stories ON taggings.story_id = stories.id WHERE tags.inactive = 0 AND stories.user_id = ? GROUP BY tags.id, stories.user_id ORDER BY `count` DESC LIMIT 1"';
CREATE VIEW q30 AS '"SELECT comments.* FROM comments WHERE comments.is_deleted = 0 AND comments.is_moderated = 0 ORDER BY id DESC LIMIT 40"';
CREATE VIEW q35 AS '"SELECT stories.*, upvotes - downvotes AS saldo FROM stories WHERE stories.merged_story_id IS NULL AND stories.is_expired = 0 ORDER BY id DESC LIMIT 51"';
-- Needs nested views to support the query as is without flattening views:
-- CREATE VIEW q36 AS '"SELECT COUNT(*) FROM replying_comments_for_count WHERE replying_comments_for_count.user_id = ?"';
CREATE VIEW q36 AS \
	'"SELECT read_ribbons.user_id, COUNT(*) \
	FROM read_ribbons \
	JOIN stories ON (read_ribbons.story_id = stories.id) \
	JOIN comments ON (read_ribbons.story_id = comments.story_id) \
	LEFT JOIN comments AS parent_comments \
	ON (comments.parent_comment_id = parent_comments.id) \
	WHERE read_ribbons.is_following = 1 \
	AND comments.user_id <> read_ribbons.user_id \
	AND comments.is_deleted = 0 \
	AND comments.is_moderated = 0 \
	AND ( comments.upvotes - comments.downvotes ) >= 0 \
	AND read_ribbons.updated_at < comments.created_at \
	AND ( \
     ( \
            parent_comments.user_id = read_ribbons.user_id \
            AND \
            ( parent_comments.upvotes - parent_comments.downvotes ) >= 0 \
     ) \
     OR \
     ( \
            parent_comments.id IS NULL \
            AND \
            stories.user_id = read_ribbons.user_id \
     ) \
     ) GROUP BY read_ribbons.user_id"';
