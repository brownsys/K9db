INSERT INTO users VALUES (0, 'joe', 'joe@gmail.com', 'super_secure_password', '2021-04-21 01:00:00', 0, 'token', 'token2', 'aboutme', 0, 0, 0, 'token', 'token', 0, 0, '2021-04-21 01:00:00', 250, 'reason','2021-04-21 01:00:00', '2021-04-21 01:00:00', 250, 'reason', 'settings');
INSERT INTO users VALUES (1, 'jeff', 'jeff@gmail.com', 'super_secure_password2', '2021-04-21 01:00:00', 0, 'token', 'token2', 'aboutme', 0, 0, 0, 'token', 'token', 0, 0, '2021-04-21 01:00:00', 250, 'reason','2021-04-21 01:00:00', '2021-04-21 01:00:00', 250, 'reason', 'settings');
INSERT INTO users VALUES (2, 'james', 'james@gmail.com', 'super_secure_password3', '2021-04-21 01:00:00', 0, 'token', 'token', 'aboutme', 0, 0, 0, 'token', 'token', 0, 0, '2021-04-21 01:00:00', 250, 'reason','2021-04-21 01:00:00', '2021-04-21 01:00:00', 250, 'reason', 'settings');
INSERT INTO comments VALUES (0, '2021-04-21 01:00:00', '2021-04-21 01:00:00', 'comment0', 0, 0, 0, 0, 'comment', 0, 0, 0, 'markeddown', 0, 0, 0, 0);
INSERT INTO comments VALUES (1, '2021-04-21 01:00:00', '2021-04-21 01:00:00', 'comment1', 1, 1, 0, 0, 'comment', 0, 0, 0, 'markeddown', 0, 0, 0, 0);
INSERT INTO comments VALUES (2, '2021-04-21 01:00:00', '2021-04-21 01:00:00', 'comment2', 2, 2, 0, 0, 'comment', 0, 0, 0, 'markeddown', 0, 0, 0, 0);
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