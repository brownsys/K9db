#!/bin/bash
mariadb --host=127.0.0.1 --port=10001 < lobster_schema_simplified.sql &&
mariadb --host=127.0.0.1 --port=10001 < social_chat_schema.sql &&
mariadb --host=127.0.0.1 --port=10001 < lobster_views.sql &&
sleep 5
mariadb --host=127.0.0.1 --port=10001 < social_chat_inserts.sql &
mariadb --host=127.0.0.1 --port=10001 < lobster_inserts.sql &
mariadb --host=127.0.0.1 --port=10001 < social_chat_queries.sql &
mariadb --host=127.0.0.1 --port=10001 < lobster_queries.sql
