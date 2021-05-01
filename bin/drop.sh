#!/bin/bash
for DATABASE in $(mysql -u $1 -p$2 <<-END_SQL
-- Prevent truncation
SET SESSION group_concat_max_len = 10000000;
-- AS '' does not output the header of the query
SELECT DISTINCT table_schema AS '' FROM information_schema.tables WHERE table_schema NOT IN ('mysql', 'information_schema', 'sys', 'performance_schema');  
END_SQL
)
do
  echo "Dropping database: $DATABASE"
  mysql -u $1 -p$2 --execute="DROP DATABASE $DATABASE;" 2>&1 | grep -v "Using a password on the command line"
done

echo "Dropped DBs!"
exit 0
