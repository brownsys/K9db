#!/bin/bash
mariadb --host=127.0.0.1 --port=10001 < creates_unique1.sql &
mariadb --host=127.0.0.1 --port=10001 < creates_unique2.sql &
# mariadb --host=127.0.0.1 --port=10001 < creates_duplicates1.sql 
# mariadb --host=127.0.0.1 --port=10001 < creates_duplicates2.sql 
# sleep 5
# ./drop-db.sh root password