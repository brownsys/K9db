#!/bin/bash
mariadb --host=127.0.0.1 --port=10001 < creates_duplicates1.sql &
mariadb --host=127.0.0.1 --port=10001 < creates_duplicates2.sql
# ./drop-db.sh root password