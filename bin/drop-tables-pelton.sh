#!/bin/bash
for TABLE in $(mariadb -A -u $1 -p$2 <<-END_SQL
USE pelton;
-- Prevent truncation
SET SESSION group_concat_max_len = 10000000;
-- AS '' does not output the header of the query
SHOW TABLES;
END_SQL
)
do
  mariadb -A -u $1 -p$2 -D pelton --execute="DROP TABLE $TABLE;"
done

echo "Dropped DBs!"
exit 0
