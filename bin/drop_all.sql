-- Prevent truncation
SET SESSION group_concat_max_len = 10000000;

SELECT GROUP_CONCAT(DISTINCT CONCAT('DROP DATABASE ', table_schema) SEPARATOR ';') FROM information_schema.tables WHERE table_schema NOT IN ('mysql', 'information_schema', 'sys', 'performance_schema');  
