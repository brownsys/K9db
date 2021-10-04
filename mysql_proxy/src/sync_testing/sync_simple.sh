#!/bin/bash
mariadb --host=127.0.0.1 --port=10001 < social_chat_schema.sql &&
sleep 5
mariadb --host=127.0.0.1 --port=10001 < social_chat_inserts.sql &
mariadb --host=127.0.0.1 --port=10001 < social_chat_queries_simple.sql
# add parallelism &
# add more inserts and more queries
# 1000 inserts 
# ignore data flow, don't use views

# ask about moving sync_testing folder
# change the way rust threadsan compiler flag is added 
# how do I know what needs to be locked? I don't understand the sharding
# run the google style script before push!