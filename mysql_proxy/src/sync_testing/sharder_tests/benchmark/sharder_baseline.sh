#!/bin/bash
time mariadb --host=127.0.0.1 --port=10001 < inserts_existing_shard_all.sql
# ./drop-db.sh root password