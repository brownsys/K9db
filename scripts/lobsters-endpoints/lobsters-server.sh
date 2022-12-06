#!/bin/bash
sudo service mariadb restart

# Wait for kill signal.
cd /home/pelton/pelton && ./run.sh opt --hostname="$LOCAL_IP:10001"
sudo service mariadb stop

# Run pelton
echo "Pelton (encrypted)"
cd /home/pelton/pelton && ./run.sh opt --hostname="$LOCAL_IP:10001"
echo "Done!"

# Run pelton (unencrypted)
echo "Pelton (unencrypted)"
cd /home/pelton/pelton && ./run.sh unencrypted --hostname="$LOCAL_IP:10001"
echo "Terminated"
