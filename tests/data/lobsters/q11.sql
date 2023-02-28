# TODO(babman): Similar to `one` but this time with merged_story_id
#
SELECT stories.id, stories.merged_story_id FROM stories WHERE stories.merged_story_id = 0;
SELECT * FROM q11 WHERE merged_story_id = 0;
