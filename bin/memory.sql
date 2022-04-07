SELECT table_schema "database", sum(data_length + index_length)/1024/1024 "size in MB" FROM information_schema.TABLES GROUP BY table_schema

