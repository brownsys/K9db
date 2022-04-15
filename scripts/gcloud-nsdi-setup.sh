#!/bin/bash

# Make sure you run as user pelton
if [[ $(whoami) != "pelton" ]]; then
  echo "MUST RUN THIS SCRIPT AS USER PELTON"
  exit 1
fi

# Go to home directory.
cd ~

# Install PERF.
sudo apt-get install linux-tools-common linux-tools-generic linux-tools-`uname -r`

# Download flamegraph repo.
git clone https://github.com/brendangregg/FlameGraph

# Make sure ssh deploy key is added.
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/nsdi

# Clones to /home/pelton/pelton
git clone git@github.com:brownsys/pelton.git
cd pelton

# Run the setup scripts
./scripts/setup-user.sh

# Do this on the server only: https://cloud.google.com/architecture/mysql-remote-access
LOCAL_IP=$(curl  http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip -H "Metadata-Flavor: Google")
sudo sed -i "s|bind-address.*|bind-address = $LOCAL_IP|" /etc/mysql/mariadb.conf.d/50-server.cnf
sudo service mariadb restart

# Do this on the client only
sudo service mariadb stop