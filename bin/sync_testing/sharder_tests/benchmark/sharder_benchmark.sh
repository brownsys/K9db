#!/bin/bash
# benchmark inserting into existing shards, as well as adding new 
# students and assignments without submissions (so no new shards created)
mariadb --host=127.0.0.1 --port=10001 < inserts_existing_shard1.sql &
mariadb --host=127.0.0.1 --port=10001 < inserts_existing_shard2.sql &
mariadb --host=127.0.0.1 --port=10001 < inserts_existing_shard3.sql &
mariadb --host=127.0.0.1 --port=10001 < inserts_existing_shard4.sql 