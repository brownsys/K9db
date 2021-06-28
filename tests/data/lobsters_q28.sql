CREATE VIEW q28 AS '"SELECT tags.id, tags.tag, tags.description, tags.privileged, tags.is_media, tags.inactive, tags.hotness_mod FROM tags WHERE tags.id = 0"';
SELECT * FROM q28;
