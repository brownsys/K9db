#!/bin/bash
IFS=';' read -ra ADDR <<< $(mysql -u $1 -p$2 < drop_all.sql | tail -n 1)
for i in "${ADDR[@]}"; do
  mysql -u $1 -p$2 --execute="$i"
done
