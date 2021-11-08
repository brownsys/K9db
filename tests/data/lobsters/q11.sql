# TODO(babman): Similar to `one` but this time with merged_story_id
#CREATE VIEW q11 AS '"SELECT stories.id FROM stories WHERE stories.merged_story_id = ?"';
SELECT * FROM q11 WHERE merged_story_id = 0;
