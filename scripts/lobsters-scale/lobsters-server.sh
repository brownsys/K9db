#!/bin/bash
# Stop mariadb.
sudo service mariadb stop
cd /home/pelton/pelton

# Run pelton server 5 times!
./run.sh opt --hostname="$LOCAL_IP:10001"
echo "Terminated"
./run.sh opt --hostname="$LOCAL_IP:10001"
echo "Terminated"
./run.sh opt --hostname="$LOCAL_IP:10001"
echo "Terminated"
./run.sh opt --hostname="$LOCAL_IP:10001"
echo "Terminated"
./run.sh opt --hostname="$LOCAL_IP:10001"
echo "Terminated"
