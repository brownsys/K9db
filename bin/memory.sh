#!/bin/bash
mariadb -u root -ppassword --execute="SELECT table_schema AS 'Database',  ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS 'Size (MB)' FROM information_schema.TABLES GROUP BY table_schema"
