#!/bin/bash
time mariadb --host=127.0.0.1 --port=10001 < inserts_existing_shard1.sql &
time mariadb --host=127.0.0.1 --port=10001 < inserts_existing_shard2.sql &
time mariadb --host=127.0.0.1 --port=10001 < inserts_existing_shard3.sql &
time mariadb --host=127.0.0.1 --port=10001 < inserts_existing_shard4.sql 
# ./drop-db.sh root password