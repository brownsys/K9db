#!/bin/bash
mariadb --host=127.0.0.1 --port=10001 < lobsters_schema_simplified.sql &&
mariadb --host=127.0.0.1 --port=10001 < lobsters_views.sql &&
sleep 5
mariadb --host=127.0.0.1 --port=10001 < lobsters_inserts1.sql &
mariadb --host=127.0.0.1 --port=10001 < lobsters_inserts2.sql 
mariadb --host=127.0.0.1 --port=10001 < lobsters_queries1.sql &
mariadb --host=127.0.0.1 --port=10001 < lobsters_queries2.sql
