CREATE VIEW q29 AS '"SELECT replying_comments_for_count.user_id, count(*) AS notifications FROM replying_comments_for_count WHERE replying_comments_for_count.user_id = 0"';
SELECT * FROM q29;
