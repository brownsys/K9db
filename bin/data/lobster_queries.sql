--skip--CREATE VIEW q1 AS '"SELECT 1 AS `one` FROM users WHERE users.PII_username = 'joe'"';
--skip--SELECT * FROM q1;

--skip--CREATE VIEW q2 AS '"SELECT 1 AS `one` FROM stories WHERE stories.short_id = 'joe'"';
--skip--SELECT * FROM q2;

--skip--CREATE VIEW q3 AS '"SELECT tags.id, tags.tag, tags.description, tags.privileged, tags.is_media, tags.inactive, tags.hotness_mod FROM tags WHERE tags.inactive = 0 AND tags.tag = 'mytag'"';
--skip--SELECT * FROM q3;

--skip--CREATE VIEW q4 AS '"SELECT keystores.`key`, keystores.`value` FROM keystores WHERE keystores.`key` = 0"';
--skip--SELECT * FROM q4;

--skip--CREATE VIEW q5 AS '"SELECT votes.id, votes.user_id, votes.story_id, votes.comment_id, votes.vote, votes.reason FROM votes WHERE votes.user_id = 0 AND votes.story_id = 0 AND votes.comment_id IS NULL"';
--skip--SELECT * FROM q5;

--skip--CREATE VIEW q6 AS '"SELECT comments.upvotes, comments.downvotes FROM comments JOIN stories ON stories.id = comments.story_id WHERE comments.story_id = 0 AND comments.user_id != stories.user_id"';
--skip--SELECT * FROM q6;

--skip--CREATE VIEW q7 AS '"SELECT stories.id, stories.created_at, stories.user_id, stories.url, stories.title, stories.description, stories.short_id, stories.is_expired, stories.upvotes, stories.downvotes, stories.is_moderated, stories.hotness, stories.markeddown_description, stories.story_cache, stories.comments_count, stories.merged_story_id, stories.unavailable_at, stories.twitter_id, stories.user_is_author FROM stories WHERE stories.short_id = 'joe'"';
--skip--SELECT * FROM q7;

--skip--CREATE VIEW q8 AS '"SELECT users.id, users.PII_username, users.email, users.password_digest, users.created_at, users.is_admin, users.password_reset_token, users.session_token, users.about, users.invited_by_user_id, users.is_moderator, users.pushover_mentions, users.rss_token, users.mailing_list_token, users.mailing_list_mode, users.karma, users.banned_at, users.banned_by_user_id, users.banned_reason, users.deleted_at, users.disabled_invite_at, users.disabled_invite_by_user_id, users.disabled_invite_reason, users.settings FROM users WHERE users.id = 0"';
--skip--SELECT * FROM q8;

--skip--CREATE VIEW q9 AS '"SELECT 1 AS `one` FROM comments WHERE comments.short_id = 'comment1'"';
--skip--SELECT * FROM q9;

--skip--CREATE VIEW q10 AS '"SELECT votes.id, votes.user_id, votes.story_id, votes.comment_id, votes.vote, votes.reason FROM votes WHERE votes.user_id = 0 AND votes.story_id = 0 AND votes.comment_id = 0"';
--skip--SELECT * FROM q10;

--skip--CREATE VIEW q11 AS '"SELECT stories.id FROM stories WHERE stories.merged_story_id = 0"';
--skip--SELECT * FROM q11;

--skip--CREATE VIEW q12 AS '"SELECT comments.id, comments.created_at, comments.updated_at, comments.short_id, comments.story_id, comments.user_id, comments.parent_comment_id, comments.thread_id, comments.comment, comments.upvotes, comments.downvotes, comments.confidence, comments.markeddown_comment, comments.is_deleted, comments.is_moderated, comments.is_from_email, comments.hat_id, comments.upvotes - comments.downvotes AS saldo FROM comments WHERE comments.story_id = 0 ORDER BY saldo ASC, confidence DESC"';
--skip--SELECT * FROM q12;

--skip--CREATE VIEW q13 AS '"SELECT tags.id, tags.tag, tags.description, tags.privileged, tags.is_media, tags.inactive, tags.hotness_mod FROM tags INNER JOIN taggings ON tags.id = taggings.tag_id WHERE taggings.story_id = 0"';
--skip--SELECT * FROM q13;

--skip--CREATE VIEW q14 AS '"SELECT comments.id, comments.created_at, comments.updated_at, comments.short_id, comments.story_id, comments.user_id, comments.parent_comment_id, comments.thread_id, comments.comment, comments.upvotes, comments.downvotes, comments.confidence, comments.markeddown_comment, comments.is_deleted, comments.is_moderated, comments.is_from_email, comments.hat_id FROM comments WHERE comments.story_id = 0 AND comments.short_id = 0"';
--skip--SELECT * FROM q14;

--skip--CREATE VIEW q15 AS '"SELECT read_ribbons.id, read_ribbons.is_following, read_ribbons.created_at, read_ribbons.updated_at, read_ribbons.user_id, read_ribbons.story_id FROM read_ribbons WHERE read_ribbons.user_id = 0 AND read_ribbons.story_id = 0"';
--skip--SELECT * FROM q15;

--skip--CREATE VIEW q16 AS '"SELECT stories.id, stories.created_at, stories.user_id, stories.url, stories.title, stories.description, stories.short_id, stories.is_expired, stories.upvotes, stories.downvotes, stories.is_moderated, stories.hotness, stories.markeddown_description, stories.story_cache, stories.comments_count, stories.merged_story_id, stories.unavailable_at, stories.twitter_id, stories.user_is_author, stories.upvotes - stories.downvotes AS saldo FROM stories WHERE stories.merged_story_id IS NULL AND stories.is_expired = 0 AND saldo >= 0 ORDER BY hotness ASC LIMIT 51"';
--skip--SELECT * FROM q16;

--skip--CREATE VIEW q17 AS '"SELECT votes.id, votes.user_id, votes.story_id, votes.comment_id, votes.vote, votes.reason FROM votes WHERE votes.comment_id = 0"';
--skip--SELECT * FROM q17;

--skip--CREATE VIEW q18 AS '"SELECT hidden_stories.story_id FROM hidden_stories WHERE hidden_stories.user_id = 0"';
--skip--SELECT * FROM q18;

--skip--CREATE VIEW q19 AS '"SELECT users.id, users.PII_username, users.email, users.password_digest, users.created_at, users.is_admin, users.password_reset_token, users.session_token, users.about, users.invited_by_user_id, users.is_moderator, users.pushover_mentions, users.rss_token, users.mailing_list_token, users.mailing_list_mode, users.karma, users.banned_at, users.banned_by_user_id, users.banned_reason, users.deleted_at, users.disabled_invite_at, users.disabled_invite_by_user_id, users.disabled_invite_reason, users.settings FROM users WHERE users.PII_username = 'joe'"';
--skip--SELECT * FROM q19;

--skip--CREATE VIEW q20 AS '"SELECT hidden_stories.id, hidden_stories.user_id, hidden_stories.story_id FROM hidden_stories WHERE hidden_stories.user_id = 0 AND hidden_stories.story_id = 0"';
--skip--SELECT * FROM q20;

--skip--CREATE VIEW q21 AS '"SELECT tag_filters.id, tag_filters.created_at, tag_filters.updated_at, tag_filters.user_id, tag_filters.tag_id FROM tag_filters WHERE tag_filters.user_id = 0"';
--skip--SELECT * FROM q21;

--skip--CREATE VIEW q22 AS '"SELECT tags.id, count(*) AS `count` FROM taggings INNER JOIN tags ON taggings.tag_id = tags.id INNER JOIN stories ON stories.id = taggings.story_id WHERE tags.inactive = 0 AND stories.user_id = 0 GROUP BY tags.id ORDER BY `count` DESC LIMIT 1"';
--skip--SELECT * FROM q22;

--skip--CREATE VIEW q23 AS '"SELECT taggings.story_id FROM taggings WHERE taggings.story_id = 0"';
--skip--SELECT * FROM q23;

--skip--CREATE VIEW q24 AS '"SELECT saved_stories.id, saved_stories.created_at, saved_stories.updated_at, saved_stories.user_id, saved_stories.story_id FROM saved_stories WHERE saved_stories.user_id = 0 AND saved_stories.story_id = 0"';
--skip--SELECT * FROM q24;

--skip--CREATE VIEW q25 AS '"SELECT suggested_titles.id, suggested_titles.story_id, suggested_titles.user_id, suggested_titles.title FROM suggested_titles WHERE suggested_titles.story_id = 0"';
--skip--SELECT * FROM q25;

--skip--CREATE VIEW q26 AS '"SELECT taggings.id, taggings.story_id, taggings.tag_id FROM taggings WHERE taggings.story_id = 0"';
--skip--SELECT * FROM q26;

--skip--CREATE VIEW q27 AS '"SELECT 1 AS `one` FROM hats WHERE hats.OWNER_user_id = 0 LIMIT 1"';
--skip--SELECT * FROM q27;

--skip--CREATE VIEW q28 AS '"SELECT tags.id, tags.tag, tags.description, tags.privileged, tags.is_media, tags.inactive, tags.hotness_mod FROM tags WHERE tags.id = 0"';
--skip--SELECT * FROM q28;

--skip--CREATE VIEW q29 AS '"SELECT replying_comments_for_count.user_id, count(*) AS notifications FROM replying_comments_for_count WHERE replying_comments_for_count.user_id = 0"';
--skip--SELECT * FROM q29;

--skip--CREATE VIEW q30 AS '"SELECT comments.id, comments.created_at, comments.updated_at, comments.short_id, comments.story_id, comments.user_id, comments.parent_comment_id, comments.thread_id, comments.comment, comments.upvotes, comments.downvotes, comments.confidence, comments.markeddown_comment, comments.is_deleted, comments.is_moderated, comments.is_from_email, comments.hat_id FROM comments WHERE comments.is_deleted = 0 AND comments.is_moderated = 0 ORDER BY id DESC LIMIT 40"';
--skip--SELECT * FROM q30;

--skip--CREATE VIEW q31 AS '"SELECT 1 FROM hidden_stories WHERE user_id = 0 AND hidden_stories.story_id = 0"';
--skip--SELECT * FROM q31;

--skip--CREATE VIEW q32 AS '"SELECT stories.id, stories.created_at, stories.user_id, stories.url, stories.title, stories.description, stories.short_id, stories.is_expired, stories.upvotes, stories.downvotes, stories.is_moderated, stories.hotness, stories.markeddown_description, stories.story_cache, stories.comments_count, stories.merged_story_id, stories.unavailable_at, stories.twitter_id, stories.user_is_author FROM stories WHERE stories.id = 0"';
--skip--SELECT * FROM q32;

--skip--CREATE VIEW q33 AS '"SELECT votes.id, votes.user_id, votes.story_id, votes.comment_id, votes.vote, votes.reason FROM votes WHERE votes.user_id = 0 AND votes.comment_id = 0"';
--skip--SELECT * FROM q33;

--skip--CREATE VIEW q35 AS '"SELECT stories.id, stories.created_at, stories.user_id, stories.url, stories.title, stories.description, stories.short_id, stories.is_expired, stories.upvotes, stories.downvotes, stories.is_moderated, stories.hotness, stories.markeddown_description, stories.story_cache, stories.comments_count, stories.merged_story_id, stories.unavailable_at, stories.twitter_id, stories.user_is_author, upvotes - downvotes AS saldo FROM stories WHERE stories.merged_story_id IS NULL AND stories.is_expired = 0 ORDER BY id DESC LIMIT 51"';
--skip--SELECT * FROM q35;