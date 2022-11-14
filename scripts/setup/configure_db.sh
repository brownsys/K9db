#!/bin/bash
# Run Mariadb.
echo "Running mariadbd ..."
service mariadb start

# sleep until mariadb is up
sleep 10

# Setup the DB if it is the first time we run
if [[ -f "/home/configure_db.sql" ]]; then
  echo "Configuring mariadb ..."
  mariadb -u root < /home/configure_db.sql
  if [ $? -eq 0 ]; then
    echo "Configured successfully"
    rm /home/configure_db.sql
  fi
fi

# Run cargo raze.
echo "Running cargo-raze"
cd /home/pelton/pelton/proxy && /root/.cargo/bin/cargo raze && cd -
cd /home/pelton/experiments/lobsters && /root/.cargo/bin/cargo raze && cd -
cd /home/pelton/experiments/ownCloud && /root/.cargo/bin/cargo raze && cd -
cd /home/pelton/experiments/vote && /root/.cargo/bin/cargo raze && cd -
