#!/bin/bash
mariadb --host=127.0.0.1 --port=10001 < creates_unique1.sql &
mariadb --host=127.0.0.1 --port=10001 < creates_unique2.sql
sleep 5
mariadb --host=127.0.0.1 --port=10001 < inserts_unique1.sql &
mariadb --host=127.0.0.1 --port=10001 < inserts_unique2.sql
# ./drop-db.sh root password