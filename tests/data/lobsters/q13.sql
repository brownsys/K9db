# TODO(babman): similar but with taggings.story_id
# CREATE VIEW q13 AS '"SELECT tags.id, tags.tag, tags.description, tags.privileged, tags.is_media, tags.inactive, tags.hotness_mod FROM tags INNER JOIN taggings ON tags.id = taggings.tag_id WHERE taggings.story_id = 0"';
SELECT * FROM q13 WHERE story_id = 0;
