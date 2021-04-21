CREATE TABLE `users` ( \
  `id` int NOT NULL PRIMARY KEY, \
  `PII_username` varchar(50), \
  `karma` int NOT NULL \
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `comments` ( \
  `id` int NOT NULL PRIMARY KEY, \
  `created_at` datetime NOT NULL, \
  `updated_at` datetime, \
  `short_id` varchar(10) NOT NULL, \
  `story_id` int NOT NULL, \
  `OWNER_user_id` int NOT NULL, \
  `parent_comment_id` int, \
  `thread_id` int, \
  `comment` text NOT NULL, \
  `upvotes` int NOT NULL, \
  `downvotes` int NOT NULL, \
  `confidence` int NOT NULL, \
  `markeddown_comment` text, \
  `is_deleted` int, \
  `is_moderated` int, \
  `is_from_email` int, \
  `hat_id` int, \
  FOREIGN KEY (`OWNER_user_id`) REFERENCES users(id) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
