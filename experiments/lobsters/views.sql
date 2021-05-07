CREATE VIEW replying_comments_for_count AS \
	'"SELECT read_ribbons.user_id, read_ribbons.story_id, comments.id \
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
     )"';
CREATE VIEW q1 AS '"SELECT 1 AS `one`, PII_username FROM users WHERE users.PII_username = ?"';
CREATE VIEW q2 AS '"SELECT 1 AS `one`, short_id FROM stories WHERE stories.short_id = ?"';
CREATE VIEW q3 AS '"SELECT tags.* FROM tags WHERE tags.inactive = 0 AND tags.tag = ?"';
--needs support for `key` column names (backticks to escape keywords)
CREATE VIEW q4 AS '"SELECT keystores.* FROM keystores WHERE keystores.keyX = ?"';
CREATE VIEW q5 AS '"SELECT votes.* FROM votes WHERE votes.OWNER_user_id = ? AND votes.story_id = ? AND votes.comment_id IS NULL"';
CREATE VIEW q6 AS '"SELECT comments.upvotes, comments.downvotes, comments.story_id FROM comments JOIN stories ON comments.story_id = stories.id WHERE comments.story_id = ? AND comments.user_id != stories.user_id"';
CREATE VIEW q7 AS '"SELECT stories.* FROM stories WHERE stories.short_id = ?"';
CREATE VIEW q8 AS '"SELECT users.* FROM users WHERE users.id = ?"';
CREATE VIEW q9 AS '"SELECT 1 AS `one`, short_id FROM comments WHERE comments.short_id = ?"';
CREATE VIEW q10 AS '"SELECT votes.* FROM votes WHERE votes.OWNER_user_id = ? AND votes.story_id = ? AND votes.comment_id = ?"';
CREATE VIEW q11 AS '"SELECT stories.id, stories.merged_story_id FROM stories WHERE stories.merged_story_id = ?"';
CREATE VIEW q12 AS '"SELECT comments.*, comments.upvotes - comments.downvotes AS saldo FROM comments WHERE comments.story_id = ? ORDER BY saldo ASC, confidence DESC"';
CREATE VIEW q13 AS '"SELECT tags.*, taggings.story_id FROM tags INNER JOIN taggings ON tags.id = taggings.tag_id WHERE taggings.story_id = ?"';
CREATE VIEW q14 AS '"SELECT comments.* FROM comments WHERE comments.story_id = ? AND comments.short_id = ?"';
CREATE VIEW q15 AS '"SELECT read_ribbons.* FROM read_ribbons WHERE read_ribbons.user_id = ? AND read_ribbons.story_id = ?"';
--rewrote net_votes as arith expr in the WHERE clause
CREATE VIEW q16 AS '"SELECT stories.* FROM stories WHERE stories.merged_story_id IS NULL AND stories.is_expired = 0 AND stories.upvotes - stories.downvotes >= 0 ORDER BY hotness ASC LIMIT 51"';
CREATE VIEW q17 AS '"SELECT votes.* FROM votes WHERE votes.comment_id = ?"';
CREATE VIEW q18 AS '"SELECT hidden_stories.story_id, hidden_stories.user_id FROM hidden_stories WHERE hidden_stories.user_id = ?"';
CREATE VIEW q19 AS '"SELECT users.* FROM users WHERE users.PII_username = ?"';
CREATE VIEW q20 AS '"SELECT hidden_stories.* FROM hidden_stories WHERE hidden_stories.user_id = ? AND hidden_stories.story_id = ?"';
CREATE VIEW q21 AS '"SELECT tag_filters.* FROM tag_filters WHERE tag_filters.user_id = ?"';
CREATE VIEW q22 AS '"SELECT tags.id, stories.user_id, count(*) AS `count` FROM taggings INNER JOIN tags ON taggings.tag_id = tags.id INNER JOIN stories ON taggings.story_id = stories.id WHERE tags.inactive = 0 AND stories.user_id = ? GROUP BY tags.id, stories.user_id ORDER BY `count` DESC LIMIT 1"';
CREATE VIEW q23 AS '"SELECT taggings.story_id FROM taggings WHERE taggings.story_id = ?"';
CREATE VIEW q24 AS '"SELECT saved_stories.* FROM saved_stories WHERE saved_stories.user_id = ? AND saved_stories.story_id = ?"';
CREATE VIEW q25 AS '"SELECT suggested_titles.* FROM suggested_titles WHERE suggested_titles.story_id = ?"';
CREATE VIEW q26 AS '"SELECT taggings.* FROM taggings WHERE taggings.story_id = ?"';
CREATE VIEW q27 AS '"SELECT 1 AS `one`, hats.OWNER_user_id FROM hats WHERE hats.OWNER_user_id = ? LIMIT 1"';
CREATE VIEW q28 AS '"SELECT suggested_taggings.* FROM suggested_taggings WHERE suggested_taggings.story_id = ?"';
CREATE VIEW q29 AS '"SELECT tags.* FROM tags WHERE tags.id = ?"';
--needs the big matview
--CREATE VIEW q29 AS '"SELECT replying_comments_for_count.user_id, count(*) AS notifications FROM replying_comments_for_count WHERE replying_comments_for_count.user_id = ?"';
CREATE VIEW q30 AS '"SELECT comments.* FROM comments WHERE comments.is_deleted = 0 AND comments.is_moderated = 0 ORDER BY id DESC LIMIT 40"';
CREATE VIEW q31 AS '"SELECT 1, user_id, story_id FROM hidden_stories WHERE user_id = ? AND hidden_stories.story_id = ?"';
CREATE VIEW q32 AS '"SELECT stories.* FROM stories WHERE stories.id = ?"';
CREATE VIEW q33 AS '"SELECT votes.* FROM votes WHERE votes.OWNER_user_id = ? AND votes.comment_id = ?"';
CREATE VIEW q34 AS '"SELECT comments.* FROM comments WHERE comments.short_id = ?"';
CREATE VIEW q35 AS '"SELECT stories.*, upvotes - downvotes AS saldo FROM stories WHERE stories.merged_story_id IS NULL AND stories.is_expired = 0 ORDER BY id DESC LIMIT 51"';
--needs the big matview
--CREATE VIEW q36 AS '"SELECT COUNT(*) FROM replying_comments_for_count WHERE replying_comments_for_count.user_id = ?"';
CREATE VIEW q37 AS '"SELECT users.* FROM users WHERE users.id = ?"';
