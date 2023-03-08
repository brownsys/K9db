#!/bin/bash
cd /home/pelton/pelton

# Run mariadb.
sudo service mariadb start

./run.sh opt --hostname="$LOCAL_IP:10001"
echo "Baseline done!"

sudo service mariadb stop

# Run pelton many times.
./run.sh opt --hostname="$LOCAL_IP:10001"
echo "Terminated"
./run.sh opt --hostname="$LOCAL_IP:10001"
echo "Terminated"
