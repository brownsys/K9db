#!/bin/bash
mariadb --host=127.0.0.1 --port=10001 < lobster_schema_simplified.sql &&
mariadb --host=127.0.0.1 --port=10001 < lobster_views.sql &&
sleep 5
mariadb --host=127.0.0.1 --port=10001 < inserts1.sql &
mariadb --host=127.0.0.1 --port=10001 < inserts2.sql 
mariadb --host=127.0.0.1 --port=10001 < queries1.sql &
mariadb --host=127.0.0.1 --port=10001 < queries2.sql
