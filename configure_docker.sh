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
    echo "mariadb success!"
    rm /home/configure_db.sql
  fi

  # Run cargo raze.
  echo "Running cargo-raze ..."
  mkdir -p /tmp/cargo-raze/doesnt/exist/
  cd /home/k9db/k9db/proxy && /root/.cargo/bin/cargo raze && cd -
  cd /home/k9db/experiments/lobsters && /root/.cargo/bin/cargo raze && cd -
  cd /home/k9db/experiments/ownCloud && /root/.cargo/bin/cargo raze && cd -
  cd /home/k9db/experiments/vote && /root/.cargo/bin/cargo raze && cd -

  # Install plotting dependencies.
  echo "Installing plotting dependencies ..."
  cd /home/k9db/experiments/scripts/plotting
  rm -rf __pycache__ venv
  python3 -m venv venv
  . venv/bin/activate
  pip install -r requirements.txt || /bin/true
  deactivate
  cd -

  echo "Configuration done!"
fi
