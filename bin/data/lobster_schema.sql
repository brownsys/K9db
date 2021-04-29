DROP TABLE IF EXISTS `comments` CASCADE;
CREATE TABLE `comments` (
	`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
	`created_at` datetime NOT NULL,
	`updated_at` datetime,
	`short_id` varchar(10) DEFAULT '' NOT NULL,
	`story_id` int unsigned NOT NULL,
	-- OWNER (author)
	`OWNER_user_id` int unsigned NOT NULL,
	`parent_comment_id` int unsigned,
	`thread_id` int unsigned,
	`comment` mediumtext NOT NULL,
	`upvotes` int DEFAULT 0 NOT NULL,
	`downvotes` int DEFAULT 0 NOT NULL,
	`confidence` decimal(20,19) DEFAULT '0.0' NOT NULL,
	`markeddown_comment` mediumtext,
	`is_deleted` tinyint(1) DEFAULT 0,
	`is_moderated` tinyint(1) DEFAULT 0,
	`is_from_email` tinyint(1) DEFAULT 0,
	`hat_id` int,
	FULLTEXT INDEX `index_comments_on_comment`  (`comment`), 
	INDEX `confidence_idx`  (`confidence`),
	UNIQUE INDEX `short_id`  (`short_id`),
	INDEX `story_id_short_id`  (`story_id`, `short_id`),
	INDEX `thread_id`  (`thread_id`),
	INDEX `index_comments_on_user_id`  (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
DROP TABLE IF EXISTS `hat_requests` CASCADE;
CREATE TABLE `hat_requests` (
	`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	`created_at` datetime,
	`updated_at` datetime,
	-- OWNER (requester)
	`OWNER_user_id` int,
	`hat` varchar(255) COLLATE utf8mb4_general_ci,
	`link` varchar(255) COLLATE utf8mb4_general_ci,
	`comment` text COLLATE utf8mb4_general_ci
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
DROP TABLE IF EXISTS `hats` CASCADE;
CREATE TABLE `hats` (
	`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	`created_at` datetime,
	`updated_at` datetime,
	-- OWNER (hat wearer)
	`OWNER_user_id` int,
	-- OWNER (grantor)
	`OWNER_granted_by_user_id` int,
	`hat` varchar(255) NOT NULL,
	`link` varchar(255) COLLATE utf8mb4_general_ci,
	`modlog_use` tinyint(1) DEFAULT 0,
	`doffed_at` datetime
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
DROP TABLE IF EXISTS `hidden_stories` CASCADE;
CREATE TABLE `hidden_stories` (
	`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	-- OWNER (hiding user)
	`OWNER_user_id` int,
	`story_id` int,
	UNIQUE INDEX `index_hidden_stories_on_user_id_and_story_id`  (`user_id`, `story_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
DROP TABLE IF EXISTS `invitation_requests` CASCADE;
CREATE TABLE `invitation_requests` (
	`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	`code` varchar(255),
	`is_verified` tinyint(1) DEFAULT 0,
	-- OWNER? This one is tricky.
	`email` varchar(255),
	`name` varchar(255),
	`memo` text,
	`ip_address` varchar(255),
	`created_at` datetime NOT NULL,
	`updated_at` datetime NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
DROP TABLE IF EXISTS `invitations` CASCADE;
CREATE TABLE `invitations` (
	`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	-- OWNER (inviter)
	`OWNER_user_id` int,
	-- OWNER? (invitee)
	`email` varchar(255),
	`code` varchar(255),
	`created_at` datetime NOT NULL,
	`updated_at` datetime NOT NULL, `memo` mediumtext
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
DROP TABLE IF EXISTS `keystores` CASCADE;
CREATE TABLE `keystores` (
	`key` varchar(50) DEFAULT '' NOT NULL,
	`value` bigint,
	PRIMARY KEY (`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
DROP TABLE IF EXISTS `messages` CASCADE;
CREATE TABLE `messages` (
	`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
	`created_at` datetime,
	-- OWNER (sender)
	`OWNER_author_user_id` int unsigned,
	-- OWNER (recipient)
	`OWNER_recipient_user_id` int unsigned,
	`has_been_read` tinyint(1) DEFAULT 0,
	`subject` varchar(100),
	`body` mediumtext,
	`short_id` varchar(30),
	`deleted_by_author` tinyint(1) DEFAULT 0,
	`deleted_by_recipient` tinyint(1) DEFAULT 0,
	UNIQUE INDEX `random_hash`  (`short_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
DROP TABLE IF EXISTS `moderations` CASCADE;
CREATE TABLE `moderations` (
	`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	`created_at` datetime NOT NULL,
	`updated_at` datetime NOT NULL,
	-- OWNER (moderator)
	`OWNER_moderator_user_id` int,
	`story_id` int,
	`comment_id` int,
	-- OWNER? (moderated user) Does this require redaction?
	`OWNER_user_id` int,
	`action` mediumtext,
	`reason` mediumtext,
	`is_from_suggestions` tinyint(1) DEFAULT 0,
	INDEX `index_moderations_on_created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
DROP TABLE IF EXISTS `read_ribbons` CASCADE;
CREATE TABLE `read_ribbons` (
	`id` bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
	`is_following` tinyint(1) DEFAULT 1,
	`created_at` datetime NOT NULL,
	`updated_at` datetime NOT NULL,
	-- OWNER
	`OWNER_user_id` bigint,
	`story_id` bigint,
	INDEX `index_read_ribbons_on_story_id` (`story_id`),
	INDEX `index_read_ribbons_on_user_id` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
DROP TABLE IF EXISTS `saved_stories` CASCADE;
CREATE TABLE `saved_stories` (
	`id` bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
	`created_at` datetime NOT NULL,
	`updated_at` datetime NOT NULL,
	-- OWNER
	`OWNER_user_id` int,
	`story_id` int,
	UNIQUE INDEX `index_saved_stories_on_user_id_and_story_id` (`user_id`, `story_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
DROP TABLE IF EXISTS `stories` CASCADE;
CREATE TABLE `stories` (
	`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
	`created_at` datetime,
	-- OWNER (author)
	`OWNER_user_id` int unsigned,
	`url` varchar(250) DEFAULT '',
	`title` varchar(150) DEFAULT '' NOT NULL,
	`description` mediumtext,
	`short_id` varchar(6) DEFAULT '' NOT NULL,
	`is_expired` tinyint(1) DEFAULT 0 NOT NULL,
	`upvotes` int unsigned DEFAULT 0 NOT NULL,
	`downvotes` int unsigned DEFAULT 0 NOT NULL,
	`is_moderated` tinyint(1) DEFAULT 0 NOT NULL,
	`hotness` decimal(20,10) DEFAULT '0.0' NOT NULL,
	`markeddown_description` mediumtext,
	`story_cache` mediumtext,
	`comments_count` int DEFAULT 0 NOT NULL,
	`merged_story_id` int,
	`unavailable_at` datetime,
	`twitter_id` varchar(20),
	`user_is_author` tinyint(1) DEFAULT 0,
	INDEX `index_stories_on_created_at` (`created_at`),
	FULLTEXT INDEX `index_stories_on_description` (`description`),
	INDEX `hotness_idx` (`hotness`),
	INDEX `is_idxes` (`is_expired`, `is_moderated`),
	INDEX `index_stories_on_is_expired` (`is_expired`),
	INDEX `index_stories_on_is_moderated` (`is_moderated`),
	INDEX `index_stories_on_merged_story_id` (`merged_story_id`),
	UNIQUE INDEX `unique_short_id` (`short_id`),
	FULLTEXT INDEX `index_stories_on_story_cache` (`story_cache`),
	FULLTEXT INDEX `index_stories_on_title` (`title`),
	INDEX `index_stories_on_twitter_id` (`twitter_id`),
	INDEX `url` (`url`(191)),
	INDEX `index_stories_on_user_id` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
DROP TABLE IF EXISTS `suggested_taggings` CASCADE;
CREATE TABLE `suggested_taggings` (
	`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	`story_id` int,
	`tag_id` int,
	-- OWNER
	`OWNER_user_id` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
DROP TABLE IF EXISTS `suggested_titles` CASCADE;
CREATE TABLE `suggested_titles` (
	`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	`story_id` int,
	-- OWNER
	`OWNER_user_id` int,
	`title` varchar(150) COLLATE utf8mb4_general_ci DEFAULT '' NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
DROP TABLE IF EXISTS `tag_filters` CASCADE;
CREATE TABLE `tag_filters` (
	`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, 
	`created_at` datetime NOT NULL,
	`updated_at` datetime NOT NULL,
	-- OWNER
	`OWNER_user_id` int,
	`tag_id` int, 
	INDEX `user_tag_idx` (`user_id`, `tag_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
DROP TABLE IF EXISTS `taggings` CASCADE;
CREATE TABLE `taggings` (
	`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
	`story_id` int unsigned NOT NULL,
	`tag_id` int unsigned NOT NULL,
	UNIQUE INDEX `story_id_tag_id` (`story_id`, `tag_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
DROP TABLE IF EXISTS `tags` CASCADE;
CREATE TABLE `tags` (
	`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
	`tag` varchar(25) DEFAULT '' NOT NULL,
	`description` varchar(100),
	`privileged` tinyint(1) DEFAULT 0,
	`is_media` tinyint(1) DEFAULT 0,
	`inactive` tinyint(1) DEFAULT 0,
	`hotness_mod` float(24) DEFAULT 0.0,
	UNIQUE INDEX `tag` (`tag`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
DROP TABLE IF EXISTS `users` CASCADE;
CREATE TABLE `users` (
	-- PII
	`PII_id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
	`username` varchar(50) COLLATE utf8mb4_general_ci,
	`karma` int DEFAULT 0 NOT NULL,
	UNIQUE INDEX `username` (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
DROP TABLE IF EXISTS `votes` CASCADE;
CREATE TABLE `votes` (
	`id` bigint unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
	-- OWNER (voter)
	`OWNER_user_id` int unsigned NOT NULL,
	`story_id` int unsigned NOT NULL,
	`comment_id` int unsigned,
	`vote` tinyint NOT NULL,
	`reason` varchar(1),
	INDEX `index_votes_on_comment_id` (`comment_id`),
	INDEX `user_id_comment_id` (`user_id`, `comment_id`),
	INDEX `user_id_story_id` (`user_id`, `story_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;