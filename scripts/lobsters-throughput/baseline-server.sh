#!/bin/bash
sudo service mariadb stop
sudo service mariadb start

cd /home/pelton/pelton

# Wait for close signal
./run.sh opt --hostname="$LOCAL_IP:10001"
echo "Terminated"

# Stop mariadb.
sudo service mariadb stop
