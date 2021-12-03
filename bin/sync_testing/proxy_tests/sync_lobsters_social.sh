#!/bin/bash
mariadb --host=127.0.0.1 --port=10001 < lobsters_schema_simplified.sql &&
mariadb --host=127.0.0.1 --port=10001 < social_chat_schema.sql &&
mariadb --host=127.0.0.1 --port=10001 < lobsters_views.sql &&
sleep 5
mariadb --host=127.0.0.1 --port=10001 < lobsters_inserts.sql &
mariadb --host=127.0.0.1 --port=10001 < lobsters_queries.sql
