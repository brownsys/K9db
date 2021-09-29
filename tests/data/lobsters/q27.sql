# TODO(babman): OWNER_user_id.
# CREATE VIEW q27 AS '"SELECT 1 AS `one` FROM hats WHERE hats.OWNER_user_id = 0 LIMIT 1"';
SELECT * FROM q27 WHERE OWNER_user_id = 0;
