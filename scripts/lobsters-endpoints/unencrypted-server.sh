#!/bin/bash
sudo service mariadb stop

# Run pelton (unencrypted)
echo "Pelton (unencrypted)"
cd /home/pelton/pelton && ./run.sh unencrypted --hostname="$LOCAL_IP:10001"
echo "Terminated"
