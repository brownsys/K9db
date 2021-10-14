#!/bin/bash
time mariadb --host=127.0.0.1 --port=10001 < creates.sql
sleep 5
time mariadb --host=127.0.0.1 --port=10001 < inserts_new_shard.sql