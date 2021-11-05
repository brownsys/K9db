#!/bin/bash
../../drop-db.sh root password

# start proxy
bazel run //mysql_proxy/src:mysql_proxy --config=opt &
sleep 20

# create tables
mariadb --host=127.0.0.1 --port=10001 < creates.sql
sleep 10

# create student shards (by inserting submissions)
mariadb --host=127.0.0.1 --port=10001 < inserts_new_shard.sql
sleep 1000

# single threaded inserts to existing submission shards plus 
# students and assignments that don't have submissions (no new shards created)
mariadb --host=127.0.0.1 --port=10001 < inserts_existing_shard_all.sql