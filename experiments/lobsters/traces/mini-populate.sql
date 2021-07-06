# users
INSERT INTO users (id, PII_username, email, password_digest, created_at, is_admin, password_reset_token, session_token, about, invited_by_user_id,is_moderator, pushover_mentions, rss_token, mailing_list_token,mailing_list_mode, karma, banned_at, banned_by_user_id, banned_reason, deleted_at, disabled_invite_at, disabled_invite_by_user_id, disabled_invite_reason, settings) VALUES (0, '0', 'x@gmail.com', 'asdf', '2021-05-07 18:00', 0, NULL, 'session_token_0', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
INSERT INTO users (id, PII_username, email, password_digest, created_at, is_admin, password_reset_token, session_token, about, invited_by_user_id,is_moderator, pushover_mentions, rss_token, mailing_list_token,mailing_list_mode, karma, banned_at, banned_by_user_id, banned_reason, deleted_at, disabled_invite_at, disabled_invite_by_user_id, disabled_invite_reason, settings) VALUES (1, '1', 'x@gmail.com', 'asdf', '2021-05-07 18:00', 0, NULL, 'session_token_1', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
INSERT INTO users (id, PII_username, email, password_digest, created_at, is_admin, password_reset_token, session_token, about, invited_by_user_id,is_moderator, pushover_mentions, rss_token, mailing_list_token,mailing_list_mode, karma, banned_at, banned_by_user_id, banned_reason, deleted_at, disabled_invite_at, disabled_invite_by_user_id, disabled_invite_reason, settings) VALUES (2, '2', 'x@gmail.com', 'asdf', '2021-05-07 18:00', 0, NULL, 'session_token_2', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
INSERT INTO users (id, PII_username, email, password_digest, created_at, is_admin, password_reset_token, session_token, about, invited_by_user_id,is_moderator, pushover_mentions, rss_token, mailing_list_token,mailing_list_mode, karma, banned_at, banned_by_user_id, banned_reason, deleted_at, disabled_invite_at, disabled_invite_by_user_id, disabled_invite_reason, settings) VALUES (3, '3', 'x@gmail.com', 'asdf', '2021-05-07 18:00', 0, NULL, 'session_token_3', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
INSERT INTO users (id, PII_username, email, password_digest, created_at, is_admin, password_reset_token, session_token, about, invited_by_user_id,is_moderator, pushover_mentions, rss_token, mailing_list_token,mailing_list_mode, karma, banned_at, banned_by_user_id, banned_reason, deleted_at, disabled_invite_at, disabled_invite_by_user_id, disabled_invite_reason, settings) VALUES (4, '4', 'x@gmail.com', 'asdf', '2021-05-07 18:00', 0, NULL, 'session_token_6', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)

# comments
INSERT INTO comments (id, created_at, updated_at, short_id, story_id, user_id, parent_comment_id, thread_id, comment, upvotes, confidence, markeddown_comment,downvotes, is_deleted, is_moderated, is_from_email, hat_id) VALUES (1, '2021-05-07 06:48:44.403611626', '2021-05-07 06:48:44.403611626', '000008', 0, 0, NULL, NULL, 'moar benchmarking', 1, 1, '<p>moar benchmarking</p>', 0, 0, 0, 0, NULL)
INSERT INTO comments (id, created_at, updated_at, short_id, story_id, user_id, parent_comment_id, thread_id, comment, upvotes, confidence, markeddown_comment,downvotes, is_deleted, is_moderated, is_from_email, hat_id) VALUES (2, '2021-05-07 06:48:44.404603956', '2021-05-07 06:48:44.404603956', '00001a', 4, 1, NULL, NULL, 'moar benchmarking', 1, 1, '<p>moar benchmarking</p>', 0, 0, 0, 0, NULL)
INSERT INTO comments (id, created_at, updated_at, short_id, story_id, user_id, parent_comment_id, thread_id, comment, upvotes, confidence, markeddown_comment,downvotes, is_deleted, is_moderated, is_from_email, hat_id) VALUES (3, '2021-05-07 06:48:44.404580386', '2021-05-07 06:48:44.404580386', '000018', 4, 2, NULL, NULL, 'moar benchmarking', 1, 1, '<p>moar benchmarking</p>', 0, 0, 0, 0, NULL)
INSERT INTO comments (id, created_at, updated_at, short_id, story_id, user_id, parent_comment_id, thread_id, comment, upvotes, confidence, markeddown_comment,downvotes, is_deleted, is_moderated, is_from_email, hat_id) VALUES (4, '2021-05-07 06:48:44.404515831', '2021-05-07 06:48:44.404515831', '000014', 1, 3, NULL, NULL, 'moar benchmarking', 1, 1, '<p>moar benchmarking</p>', 0, 0, 0, 0, NULL)
INSERT INTO comments (id, created_at, updated_at, short_id, story_id, user_id, parent_comment_id, thread_id, comment, upvotes, confidence, markeddown_comment,downvotes, is_deleted, is_moderated, is_from_email, hat_id) VALUES (5, '2021-05-07 06:48:44.404421130', '2021-05-07 06:48:44.404421130', '00000z', 2, 4, NULL, NULL, 'moar benchmarking', 1, 1, '<p>moar benchmarking</p>', 0, 0, 0, 0, NULL)
INSERT INTO comments (id, created_at, updated_at, short_id, story_id, user_id, parent_comment_id, thread_id, comment, upvotes, confidence, markeddown_comment,downvotes, is_deleted, is_moderated, is_from_email, hat_id) VALUES (6, '2021-05-07 06:48:44.404628583', '2021-05-07 06:48:44.404628583', '000019', 1, 0, NULL, NULL, 'moar benchmarking', 1, 1, '<p>moar benchmarking</p>', 0, 0, 0, 0, NULL)
INSERT INTO comments (id, created_at, updated_at, short_id, story_id, user_id, parent_comment_id, thread_id, comment, upvotes, confidence, markeddown_comment,downvotes, is_deleted, is_moderated, is_from_email, hat_id) VALUES (7, '2021-05-07 06:48:44.404333773', '2021-05-07 06:48:44.404333773', '00000v', 1, 1, NULL, NULL, 'moar benchmarking', 1, 1, '<p>moar benchmarking</p>', 0, 0, 0, 0, NULL)
INSERT INTO comments (id, created_at, updated_at, short_id, story_id, user_id, parent_comment_id, thread_id, comment, upvotes, confidence, markeddown_comment,downvotes, is_deleted, is_moderated, is_from_email, hat_id) VALUES (8, '2021-05-07 06:48:44.404302563', '2021-05-07 06:48:44.404302563', '00000r',4, 2, NULL, NULL, 'moar benchmarking', 1, 1, '<p>moar benchmarking</p>', 0, 0, 0, 0, NULL)
INSERT INTO comments (id, created_at, updated_at, short_id, story_id, user_id, parent_comment_id, thread_id, comment, upvotes, confidence, markeddown_comment,downvotes, is_deleted, is_moderated, is_from_email, hat_id) VALUES (9, '2021-05-07 06:48:44.404185275', '2021-05-07 06:48:44.404185275', '00000n', 0, 3, NULL, NULL, 'moar benchmarking', 1, 1, '<p>moar benchmarking</p>', 0, 0, 0, 0, NULL)
INSERT INTO comments (id, created_at, updated_at, short_id, story_id, user_id, parent_comment_id, thread_id, comment, upvotes, confidence, markeddown_comment,downvotes, is_deleted, is_moderated, is_from_email, hat_id) VALUES (10, '2021-05-07 06:48:44.404149637', '2021-05-07 06:48:44.404149637', '00000l', 1, 2, NULL, NULL, 'moar benchmarking', 1, 1, '<p>moar benchmarking</p>', 0, 0, 0, 0, NULL)


# hat_requests

# hats

# hidden_stories

# invitation_requests

# invitations

# keystores

# messages

# moderations

# read_ribbons

# saved_stories

# stories
INSERT INTO stories (id, created_at, user_id, title, description, short_id, upvotes, hotness, markeddown_description,url, is_expired, downvotes, is_moderated, comments_count,story_cache, merged_story_id, unavailable_at, twitter_id, user_is_author) VALUES (1, '2021-05-07 06:47:56.945062198', 3, 'Base article 13', 'to infinity', '00000d', 1, 19217, '<p>to infinity</p>\n', '', 0, 0, 0, 0, NULL, NULL, NULL, NULL, NULL)
INSERT INTO stories (id, created_at, user_id, title, description, short_id, upvotes, hotness, markeddown_description,url, is_expired, downvotes, is_moderated, comments_count,story_cache, merged_story_id, unavailable_at, twitter_id, user_is_author) VALUES (2, '2021-05-07 06:47:56.950164118', 0, 'Base article 49', 'to infinity', '00001d', 1, 19217, '<p>to infinity</p>\n', '', 0, 0, 0, 0, NULL, NULL, NULL, NULL, NULL)
INSERT INTO stories (id, created_at, user_id, title, description, short_id, upvotes, hotness, markeddown_description,url, is_expired, downvotes, is_moderated, comments_count,story_cache, merged_story_id, unavailable_at, twitter_id, user_is_author) VALUES (3, '2021-05-07 06:47:56.950207107', 1, 'Base article 41', 'to infinity', '000015', 1, 19217, '<p>to infinity</p>\n', '', 0, 0, 0, 0, NULL, NULL, NULL, NULL, NULL)
INSERT INTO stories (id, created_at, user_id, title, description, short_id, upvotes, hotness, markeddown_description,url, is_expired, downvotes, is_moderated, comments_count,story_cache, merged_story_id, unavailable_at, twitter_id, user_is_author) VALUES (4, '2021-05-07 06:47:56.950237688', 4, 'Base article 47', 'to infinity', '00001b', 1, 19217, '<p>to infinity</p>\n', '', 0, 0, 0, 0, NULL, NULL, NULL, NULL, NULL)
INSERT INTO stories (id, created_at, user_id, title, description, short_id, upvotes, hotness, markeddown_description,url, is_expired, downvotes, is_moderated, comments_count,story_cache, merged_story_id, unavailable_at, twitter_id, user_is_author) VALUES (5, '2021-05-07 06:47:56.950364638', 4, 'Base article 39', 'to infinity', '000013', 1, 19217, '<p>to infinity</p>\n', '', 0, 0, 0, 0, NULL, NULL, NULL, NULL, NULL)
INSERT INTO stories (id, created_at, user_id, title, description, short_id, upvotes, hotness, markeddown_description,url, is_expired, downvotes, is_moderated, comments_count,story_cache, merged_story_id, unavailable_at, twitter_id, user_is_author) VALUES (6, '2021-05-07 06:47:56.950388587', 4, 'Base article 48', 'to infinity', '00001c', 1, 19217, '<p>to infinity</p>\n', '', 0, 0, 0, 0, NULL, NULL, NULL, NULL, NULL)ULL)

# tags
# Was not in population load, manually added.
INSERT INTO tags VALUES (1, 'Tag1', 'Description 1', 0, 0, 0, 0);
INSERT INTO tags VALUES (2, 'Tag2', 'Description 2', 0, 0, 0, 0);
INSERT INTO tags VALUES (3, 'Tag3', 'Description 3', 0, 0, 0, 0);

# suggested_taggings

# suggested_titles

# tag_filters

# taggings
INSERT INTO taggings (id, story_id, tag_id) VALUES (1, 1, 1)
INSERT INTO taggings (id, story_id, tag_id) VALUES (2, 2, 1)
INSERT INTO taggings (id, story_id, tag_id) VALUES (3, 3, 1)
INSERT INTO taggings (id, story_id, tag_id) VALUES (4, 4, 1)
INSERT INTO taggings (id, story_id, tag_id) VALUES (5, 1, 1)
INSERT INTO taggings (id, story_id, tag_id) VALUES (6, 1, 3)
INSERT INTO taggings (id, story_id, tag_id) VALUES (7, 2, 3)


# votes
INSERT INTO votes (id, OWNER_user_id, story_id, comment_id, vote, reason) VALUES (1, 0, 1, NULL, 1, NULL)
INSERT INTO votes (id, OWNER_user_id, story_id, comment_id, vote, reason) VALUES (2, 1, 1, NULL, 1, NULL)
INSERT INTO votes (id, OWNER_user_id, story_id, comment_id, vote, reason) VALUES (3, 2, 1, NULL, 1, NULL)
INSERT INTO votes (id, OWNER_user_id, story_id, comment_id, vote, reason) VALUES (4, 3, 1, NULL, 1, NULL)
INSERT INTO votes (id, OWNER_user_id, story_id, comment_id, vote, reason) VALUES (5, 0, 4, 3, 1, NULL)
INSERT INTO votes (id, OWNER_user_id, story_id, comment_id, vote, reason) VALUES (6, 1, 4, NULL, 1, NULL)
INSERT INTO votes (id, OWNER_user_id, story_id, comment_id, vote, reason) VALUES (7, 2, 2, NULL, 1, NULL)
INSERT INTO votes (id, OWNER_user_id, story_id, comment_id, vote, reason) VALUES (8, 2, 3, NULL, 1, NULL)
INSERT INTO votes (id, OWNER_user_id, story_id, comment_id, vote, reason) VALUES (9, 2, 4, 2, 1, NULL)
INSERT INTO votes (id, OWNER_user_id, story_id, comment_id, vote, reason) VALUES (10, 3, 2, NULL, 1, NULL)

