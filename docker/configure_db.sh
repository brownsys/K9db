#!/bin/bash
echo "Running mysqld ..."
mysqld &
MYSQLID=$!

# sleep until mysql/mariadb is up
sleep 10

if [[ -f "/home/configure_db.sql" ]]; then
  echo "Running cargo-raze" > /home/log.log
  cd /home/pelton/mysql_proxy/ && rm -rf cargo && cargo raze && cd -

  echo "Configuring mariadb ..."
  mariadb -u root < /home/configure_db.sql
  if [ $? -eq 0 ]; then
    echo "Configured successfully"
    rm /home/configure_db.sql
  fi
fi

wait $MYSQLID
echo "mysqld exited! $?"
