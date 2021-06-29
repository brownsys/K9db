CREATE VIEW q33 AS '"SELECT votes.id, votes.user_id, votes.story_id, votes.comment_id, votes.vote, votes.reason FROM votes WHERE votes.user_id = 0 AND votes.comment_id = 0"';
SELECT * FROM q33;
