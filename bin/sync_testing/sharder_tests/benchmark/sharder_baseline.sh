#!/bin/bash
# single threaded inserts to existing submission shards plus 
# students and assignments that don't have submissions (no new shards created)
mariadb --host=127.0.0.1 --port=10001 < inserts_existing_shard_all.sql