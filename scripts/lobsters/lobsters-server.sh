#!/bin/bash
sudo service mariadb restart
cd /home/pelton && ./run.sh opt --hostname="$LOCAL_IP:10001"
echo "Terminated"
