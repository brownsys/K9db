#!/bin/bash
# Run cargo raze.
echo "Running cargo-raze"
cd /home/pelton/mysql_proxy/
rm -rf cargo && /root/.cargo/bin/cargo raze
cd -

# Run Mariadb.
echo "Running mysqld ..."
mysqld &
MYSQLID=$!

# sleep until mysql/mariadb is up
sleep 10

if [[ -f "/home/configure_db.sql" ]]; then
  echo "Configuring mariadb ..."
  mariadb -u root < /home/configure_db.sql
  if [ $? -eq 0 ]; then
    echo "Configured successfully"
    rm /home/configure_db.sql
  fi
fi

wait $MYSQLID
echo "mysqld exited! $?"
