INSERT INTO users VALUES (0, 'joe', 'joe@gmail.com', 'super_secure_password', '2021-04-21 01:00:00', 0, 'token', 'token2', 'aboutme', 0, 0, 0, 'token', 'token', 0, 0, '2021-04-21 01:00:00', 250, 'reason','2021-04-21 01:00:00', '2021-04-21 01:00:00', 250, 'reason', 'settings');
INSERT INTO users VALUES (1, 'jeff', 'jeff@gmail.com', 'super_secure_password2', '2021-04-21 01:00:00', 0, 'token', 'token2', 'aboutme', 0, 0, 0, 'token', 'token', 0, 0, '2021-04-21 01:00:00', 250, 'reason','2021-04-21 01:00:00', '2021-04-21 01:00:00', 250, 'reason', 'settings');
INSERT INTO users VALUES (2, 'james', 'james@gmail.com', 'super_secure_password3', '2021-04-21 01:00:00', 0, 'token', 'token', 'aboutme', 0, 0, 0, 'token', 'token', 0, 0, '2021-04-21 01:00:00', 250, 'reason','2021-04-21 01:00:00', '2021-04-21 01:00:00', 250, 'reason', 'settings');
INSERT INTO comments VALUES (0, '2021-04-21 01:00:00', '2021-04-21 01:00:00', 'comment0', 0, 0, 0, 0, 'comment', 0, 0, 0, 'markeddown', 0, 0, 0, 0);
INSERT INTO comments VALUES (1, '2021-04-21 01:00:00', '2021-04-21 01:00:00', 'comment1', 1, 1, 0, 0, 'comment', 0, 0, 0, 'markeddown', 0, 0, 0, 0);
INSERT INTO comments VALUES (2, '2021-04-21 01:00:00', '2021-04-21 01:00:00', 'comment2', 2, 2, 0, 0, 'comment', 0, 0, 0, 'markeddown', 0, 0, 0, 0);
INSERT INTO comments VALUES (3, '2021-04-21 01:00:00', '2021-04-21 01:00:00', '', 0, 2, 5, 5, 'comment', 5, 5, 5, 'markeddown', 5, 5, 5, 5);
INSERT INTO hat_requests VALUES (0, '2021-04-21 01:00:00', '2021-04-21 01:00:00', 0, 'myhat', 'mylink', 'mycomment');
INSERT INTO hat_requests VALUES (1, '2021-04-21 01:00:00', '2021-04-21 01:00:00', 1, 'myhat', 'mylink', 'mycomment');
INSERT INTO hats VALUES (0, '2021-04-21 01:00:00', '2021-04-21 01:00:00', 0, 0, 'myhat', 'mylink', 0, '2021-04-21 01:00:00');
INSERT INTO hats VALUES (1, '2021-04-21 01:00:00', '2021-04-21 01:00:00', 1, 1, 'myhat', 'mylink', 0, '2021-04-21 01:00:00');
INSERT INTO hidden_stories VALUES (0, 0, 0);
INSERT INTO hidden_stories VALUES (1, 1, 1);
INSERT INTO invitation_requests VALUES (0, 'code', 0, 'email', 'name', 'memo', 'ip', '2021-04-21 01:00:00', '2021-04-21 01:00:00');
INSERT INTO invitation_requests VALUES (1, 'code', 1, 'email', 'name', 'memo', 'ip', '2021-04-21 01:00:00', '2021-04-21 01:00:00');
INSERT INTO invitations VALUES (0, 0, 'email', 'code', '2021-04-21 01:00:00', '2021-04-21 01:00:00', 'memo');
INSERT INTO invitations VALUES (1, 1, 'email', 'code', '2021-04-21 01:00:00', '2021-04-21 01:00:00', 'memo');
INSERT INTO keystores VALUES ('mykey', 0);
INSERT INTO keystores VALUES ('mykey2', 0);
INSERT INTO messages VALUES (0, '2021-04-21 01:00:00', 0, 1, 0, 'subject', 'body', '0', 0, 0);
INSERT INTO messages VALUES (1, '2021-04-21 01:00:00', 1, 0, 0, 'subject', 'body', '0', 0, 0);
INSERT INTO moderations VALUES (0, '2021-04-21 01:00:00', '2021-04-21 01:00:00', 0, 0, 0, 1, 'action', 'reason', 0);
INSERT INTO moderations VALUES (1, '2021-04-21 01:00:00', '2021-04-21 01:00:00', 1, 1, 1, 2, 'action', 'reason', 0);
INSERT INTO read_ribbons VALUES (0, 0, '2021-04-21 01:00:00', '2021-04-21 01:00:00', 0, 0);
INSERT INTO read_ribbons VALUES (1, 0, '2021-04-21 01:00:00', '2021-04-21 01:00:00', 1, 1);
INSERT INTO saved_stories VALUES (0, '2021-04-21 01:00:00', '2021-04-21 01:00:00', 0, 0);
INSERT INTO saved_stories VALUES (1, '2021-04-21 01:00:00', '2021-04-21 01:00:00', 1, 1);
INSERT INTO stories VALUES (0,'2021-04-21 01:00:00', 0, 'url', 'title', 'description', 'joe', 0, 50, 20, 0, 1, 'markeddown', 'cache', 0, 0, '2021-04-21 01:00:00', 'twitter', 1);
INSERT INTO stories VALUES (1,'2021-04-21 01:00:00', 1, 'url2', 'title2', 'description2', 'jeff', 0, 50, 20, 0, 1, 'markeddown', 'cache', 0, 0, '2021-04-21 01:00:00', 'twitter', 1);
INSERT INTO stories VALUES (2,'2021-04-21 01:00:00', 2, 'url2', 'title2', 'description2', 'james', 0, 50, 20, 0, 1, 'markeddown', 'cache', 0, 0, '2021-04-21 01:00:00', 'twitter', 1);
INSERT INTO stories VALUES (3,'2021-04-21 01:00:00', 2, 'url2', 'title3', 'description2', 'james', 0, 50, 20, 0, 1, 'markeddown', 'cache', 0, NULL, '2021-04-21 01:00:00', 'twitter', 1);
INSERT INTO suggested_taggings VALUES (0, 0, 0, 0);
INSERT INTO suggested_taggings VALUES (1, 1, 1, 1);
INSERT INTO suggested_titles VALUES (0, 0, 0, 'title');
INSERT INTO suggested_titles VALUES (1, 1, 1, 'title2');
INSERT INTO tag_filters VALUES (0, '2021-04-21 01:00:00', '2021-04-21 01:00:00', 0, 0);
INSERT INTO tag_filters VALUES (1, '2021-04-21 01:00:00', '2021-04-21 01:00:00', 1, 1);
INSERT INTO taggings VALUES (0, 0, 0);
INSERT INTO taggings VALUES (1, 1, 1);
INSERT INTO taggings VALUES (2, 2, 2);
INSERT INTO tags VALUES (0, 'mytag', 'description', 0, 0, 0, 0);
INSERT INTO tags VALUES (1, 'mytag2', 'description2', 0, 0, 0, 0);
INSERT INTO tags VALUES (2, 'mytag3', 'description3', 0, 0, 0, 0);
INSERT INTO votes VALUES (0, 0, 0, 0, 1, 'r');
INSERT INTO votes VALUES (1, 1, 1, 1, 1, 'r');
INSERT INTO votes VALUES (2, 2, 2, 2, 1, 'r');
INSERT INTO votes VALUES (3, 0, 0, NULL, 1, 'r');
--
CREATE VIEW q1 AS SELECT 1 AS one FROM users WHERE users.PII_username = "joe";
SELECT * FROM q1;
-- 
CREATE VIEW q2 AS SELECT 1 AS one FROM stories WHERE stories.short_id = "joe";
SELECT * FROM q2;
-- 
CREATE VIEW q3 AS SELECT tags.id, tags.tag, tags.description, tags.privileged, tags.is_media, tags.inactive, tags.hotness_mod FROM tags WHERE tags.inactive = 0 AND tags.tag = 'mytag';
SELECT * FROM q3;
-- 
CREATE VIEW q4 AS SELECT keystores.`key`, keystores.`value` FROM keystores WHERE keystores.`key` = 0;
SELECT * FROM q4;
--
CREATE VIEW q5 AS SELECT votes.id, votes.user_id, votes.story_id, votes.comment_id, votes.vote, votes.reason FROM votes WHERE votes.user_id = 0 AND votes.story_id = 0 AND votes.comment_id IS NULL;
SELECT * FROM q5;
-- 
CREATE VIEW q6 AS SELECT comments.upvotes, comments.downvotes FROM comments JOIN stories ON stories.id = comments.story_id WHERE comments.story_id = 0 AND comments.user_id != stories.user_id;
SELECT * FROM q6;
--
CREATE VIEW q7 AS SELECT stories.id, stories.created_at, stories.user_id, stories.url, stories.title, stories.description, stories.short_id, stories.is_expired, stories.upvotes, stories.downvotes, stories.is_moderated, stories.hotness, stories.markeddown_description, stories.story_cache, stories.comments_count, stories.merged_story_id, stories.unavailable_at, stories.twitter_id, stories.user_is_author FROM stories WHERE stories.short_id = 'joe';
SELECT * FROM q7;
-- 
CREATE VIEW q8 AS SELECT users.id, users.PII_username, users.email, users.password_digest, users.created_at, users.is_admin, users.password_reset_token, users.session_token, users.about, users.invited_by_user_id, users.is_moderator, users.pushover_mentions, users.rss_token, users.mailing_list_token, users.mailing_list_mode, users.karma, users.banned_at, users.banned_by_user_id, users.banned_reason, users.deleted_at, users.disabled_invite_at, users.disabled_invite_by_user_id, users.disabled_invite_reason, users.settings FROM users WHERE users.id = 0;
SELECT * FROM q8;
-- 
CREATE VIEW q9 AS SELECT 1 AS `one` FROM comments WHERE comments.short_id = 'comment1';
SELECT * FROM q9;
--
CREATE VIEW q10 AS SELECT votes.id, votes.user_id, votes.story_id, votes.comment_id, votes.vote, votes.reason FROM votes WHERE votes.user_id = 0 AND votes.story_id = 0 AND votes.comment_id = 0;
SELECT * FROM q10;
--
CREATE VIEW q11 AS SELECT stories.id FROM stories WHERE stories.merged_story_id = 0;
SELECT * FROM q11;

-- ORDER BY
CREATE VIEW q12 AS SELECT comments.id, comments.created_at, comments.updated_at, comments.short_id, comments.story_id, comments.user_id, comments.parent_comment_id, comments.thread_id, comments.comment, comments.upvotes, comments.downvotes, comments.confidence, comments.markeddown_comment, comments.is_deleted, comments.is_moderated, comments.is_from_email, comments.hat_id, comments.upvotes - comments.downvotes AS saldo FROM comments WHERE comments.story_id = 2 ORDER BY saldo ASC, confidence DESC;
SELECT * FROM q12;
-- JOIN
CREATE VIEW q13 AS SELECT tags.id, tags.tag, tags.description, tags.privileged, tags.is_media, tags.inactive, tags.hotness_mod FROM tags INNER JOIN taggings ON tags.id = taggings.tag_id WHERE taggings.story_id = 2;
SELECT * FROM q13;
--
CREATE VIEW q14 AS SELECT comments.id, comments.created_at, comments.updated_at, comments.short_id, comments.story_id, comments.user_id, comments.parent_comment_id, comments.thread_id, comments.comment, comments.upvotes, comments.downvotes, comments.confidence, comments.markeddown_comment, comments.is_deleted, comments.is_moderated, comments.is_from_email, comments.hat_id FROM comments WHERE comments.story_id = 0 AND comments.short_id = '';
SELECT * FROM q14;
--
CREATE VIEW q15 AS SELECT read_ribbons.id, read_ribbons.is_following, read_ribbons.created_at, read_ribbons.updated_at, read_ribbons.user_id, read_ribbons.story_id FROM read_ribbons WHERE read_ribbons.user_id = 0 AND read_ribbons.story_id = 0;
SELECT * FROM q15;
-- ORDER BY
CREATE VIEW q16 AS SELECT stories.id, stories.created_at, stories.user_id, stories.url, stories.title, stories.description, stories.short_id, stories.is_expired, stories.upvotes, stories.downvotes, stories.is_moderated, stories.hotness, stories.markeddown_description, stories.story_cache, stories.comments_count, stories.merged_story_id, stories.unavailable_at, stories.twitter_id, stories.user_is_author, stories.upvotes - stories.downvotes AS saldo FROM stories WHERE stories.merged_story_id IS NULL AND stories.is_expired = 0 AND saldo >= 0 ORDER BY hotness ASC LIMIT 51;
SELECT * FROM q16;
--
CREATE VIEW q17 AS SELECT votes.id, votes.user_id, votes.story_id, votes.comment_id, votes.vote, votes.reason FROM votes WHERE votes.comment_id = 0;
SELECT * FROM q17;
--
CREATE VIEW q18 AS SELECT hidden_stories.story_id FROM hidden_stories WHERE hidden_stories.user_id = 0;
SELECT * FROM q18;
--
CREATE VIEW q19 AS SELECT users.id, users.PII_username, users.email, users.password_digest, users.created_at, users.is_admin, users.password_reset_token, users.session_token, users.about, users.invited_by_user_id, users.is_moderator, users.pushover_mentions, users.rss_token, users.mailing_list_token, users.mailing_list_mode, users.karma, users.banned_at, users.banned_by_user_id, users.banned_reason, users.deleted_at, users.disabled_invite_at, users.disabled_invite_by_user_id, users.disabled_invite_reason, users.settings FROM users WHERE users.PII_username = 'joe';
SELECT * FROM q19;
--
CREATE VIEW q20 AS SELECT hidden_stories.id, hidden_stories.user_id, hidden_stories.story_id FROM hidden_stories WHERE hidden_stories.user_id = 0 AND hidden_stories.story_id = 0;
SELECT * FROM q20;
--
CREATE VIEW q21 AS SELECT tag_filters.id, tag_filters.created_at, tag_filters.updated_at, tag_filters.user_id, tag_filters.tag_id FROM tag_filters WHERE tag_filters.user_id = 0;
SELECT * FROM q21;

-- ORDER BY
CREATE VIEW q22 AS SELECT tags.id, count(*) AS `count` FROM taggings INNER JOIN tags ON taggings.tag_id = tags.id INNER JOIN stories ON stories.id = taggings.story_id WHERE tags.inactive = 0 AND stories.user_id = 0 GROUP BY tags.id ORDER BY `count` DESC LIMIT 1;
SELECT * FROM q22;
--
CREATE VIEW q23 AS SELECT taggings.story_id FROM taggings WHERE taggings.story_id = 0;
SELECT * FROM q23;
--
CREATE VIEW q24 AS SELECT saved_stories.id, saved_stories.created_at, saved_stories.updated_at, saved_stories.user_id, saved_stories.story_id FROM saved_stories WHERE saved_stories.user_id = 0 AND saved_stories.story_id = 0;
SELECT * FROM q24;
--
CREATE VIEW q25 AS SELECT suggested_titles.id, suggested_titles.story_id, suggested_titles.user_id, suggested_titles.title FROM suggested_titles WHERE suggested_titles.story_id = 0;
SELECT * FROM q25;
--
CREATE VIEW q26 AS SELECT taggings.id, taggings.story_id, taggings.tag_id FROM taggings WHERE taggings.story_id = 0;
SELECT * FROM q26;
--
CREATE VIEW q27 AS SELECT 1 AS `one` FROM hats WHERE hats.OWNER_user_id = 0 LIMIT 1;
SELECT * FROM q27;
--
CREATE VIEW q28 AS SELECT tags.id, tags.tag, tags.description, tags.privileged, tags.is_media, tags.inactive, tags.hotness_mod FROM tags WHERE tags.id = 0;
SELECT * FROM q28;
--
CREATE VIEW q29 AS SELECT replying_comments_for_count.user_id, count(*) AS notifications FROM replying_comments_for_count WHERE replying_comments_for_count.user_id = 0;
SELECT * FROM q29;

-- ORDER BY
CREATE VIEW q30 AS SELECT comments.id, comments.created_at, comments.updated_at, comments.short_id, comments.story_id, comments.user_id, comments.parent_comment_id, comments.thread_id, comments.comment, comments.upvotes, comments.downvotes, comments.confidence, comments.markeddown_comment, comments.is_deleted, comments.is_moderated, comments.is_from_email, comments.hat_id FROM comments WHERE comments.is_deleted = 0 AND comments.is_moderated = 0 ORDER BY id DESC LIMIT 40;
SELECT * FROM q30;
--
CREATE VIEW q31 AS SELECT 1 FROM hidden_stories WHERE user_id = 0 AND hidden_stories.story_id = 0;
SELECT * FROM q31;
--
CREATE VIEW q32 AS SELECT stories.id, stories.created_at, stories.user_id, stories.url, stories.title, stories.description, stories.short_id, stories.is_expired, stories.upvotes, stories.downvotes, stories.is_moderated, stories.hotness, stories.markeddown_description, stories.story_cache, stories.comments_count, stories.merged_story_id, stories.unavailable_at, stories.twitter_id, stories.user_is_author FROM stories WHERE stories.id = 0;
SELECT * FROM q32;
--
CREATE VIEW q33 AS SELECT votes.id, votes.user_id, votes.story_id, votes.comment_id, votes.vote, votes.reason FROM votes WHERE votes.user_id = 0 AND votes.comment_id = 0;
SELECT * FROM q33;
--
CREATE VIEW q34 AS SELECT comments.id, comments.created_at, comments.updated_at, comments.short_id, comments.story_id, comments.user_id, comments.parent_comment_id, comments.thread_id, comments.comment, comments.upvotes, comments.downvotes, comments.confidence, comments.markeddown_comment, comments.is_deleted, comments.is_moderated, comments.is_from_email, comments.hat_id FROM comments WHERE comments.short_id = 0;
SELECT * FROM q34;

-- ORDER BY
CREATE VIEW q35 AS SELECT stories.id, stories.created_at, stories.user_id, stories.url, stories.title, stories.description, stories.short_id, stories.is_expired, stories.upvotes, stories.downvotes, stories.is_moderated, stories.hotness, stories.markeddown_description, stories.story_cache, stories.comments_count, stories.merged_story_id, stories.unavailable_at, stories.twitter_id, stories.user_is_author, upvotes - downvotes AS saldo FROM stories WHERE stories.merged_story_id IS NULL AND stories.is_expired = 0 ORDER BY id DESC LIMIT 51;
SELECT * FROM q35;

SELECT * FROM q1 INTO OUTFILE '/tmp/q1.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q2 INTO OUTFILE '/tmp/q2.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q3 INTO OUTFILE '/tmp/q3.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q4 INTO OUTFILE '/tmp/q4.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q5 INTO OUTFILE '/tmp/q5.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q6 INTO OUTFILE '/tmp/q6.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q7 INTO OUTFILE '/tmp/q7.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q8 INTO OUTFILE '/tmp/q8.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q9 INTO OUTFILE '/tmp/q9.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q10 INTO OUTFILE '/tmp/q10.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q11 INTO OUTFILE '/tmp/q11.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q12 INTO OUTFILE '/tmp/q12.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q13 INTO OUTFILE '/tmp/q13.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q14 INTO OUTFILE '/tmp/q14.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q15 INTO OUTFILE '/tmp/q15.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q16 INTO OUTFILE '/tmp/q16.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q17 INTO OUTFILE '/tmp/q17.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q18 INTO OUTFILE '/tmp/q18.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q19 INTO OUTFILE '/tmp/q19.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q20 INTO OUTFILE '/tmp/q20.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q21 INTO OUTFILE '/tmp/q21.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q22 INTO OUTFILE '/tmp/q22.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q23 INTO OUTFILE '/tmp/q23.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q24 INTO OUTFILE '/tmp/q24.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q25 INTO OUTFILE '/tmp/q25.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q26 INTO OUTFILE '/tmp/q26.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q27 INTO OUTFILE '/tmp/q27.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q28 INTO OUTFILE '/tmp/q28.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q29 INTO OUTFILE '/tmp/q29.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q30 INTO OUTFILE '/tmp/q30.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q31 INTO OUTFILE '/tmp/q31.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q32 INTO OUTFILE '/tmp/q32.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q33 INTO OUTFILE '/tmp/q33.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q34 INTO OUTFILE '/tmp/q34.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';
SELECT * FROM q35 INTO OUTFILE '/tmp/q35.txt' FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\n';