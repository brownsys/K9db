# TODO(babman): SIMILAR THING BUT WITH user_id
# CREATE VIEW q22 AS '"SELECT tags.id, count(*) AS `count` FROM taggings INNER JOIN tags ON taggings.tag_id = tags.id INNER JOIN stories ON taggings.story_id = stories.id WHERE tags.inactive = 0 AND stories.user_id = 0 GROUP BY tags.id ORDER BY `count` DESC LIMIT 1"';
SELECT * FROM q22 WHERE user_id = 0;
SELECT tags.id, stories.user_id, count(*) AS `count` FROM taggings INNER JOIN tags ON taggings.tag_id = tags.id INNER JOIN stories ON taggings.story_id = stories.id WHERE tags.inactive = 0 AND stories.user_id = 0 GROUP BY tags.id, stories.user_id ORDER BY `count` DESC LIMIT 1;
