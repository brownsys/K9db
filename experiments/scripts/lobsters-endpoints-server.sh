#!/bin/bash
mkdir -p /mnt/disks/my-ssd/k9db

DB_IP=$LOCAL_IP
if [[ $LOCAL_IP == "" ]]; then
  DB_IP="127.0.0.1"
fi

echo "Check load instance for output"
echo "Running server on $DB_IP:10001"

# Restart mariadb.
# Load script will now run the baseline load.
echo "MariaDB Baseline"
sudo service mariadb restart
./run.sh opt --hostname="$DB_IP:10001"  # wait for kill signal from load

# Baseline load done.
sudo service mariadb stop

# Run encrypted K9db.
echo "K9db (encrypted)"
rm -rf /mnt/disks/my-ssd/k9db/k9db
bazel run //:k9db --config=opt -- --hostname="$DB_IP:10001" --db_path="/mnt/disks/my-ssd/k9db/"

# Run unencrypted K9db.
echo "K9db (unencrypted)"
rm -rf /mnt/disks/my-ssd/k9db/k9db
bazel run //:k9db --config=opt --encryption=off -- --hostname="$DB_IP:10001" --db_path="/mnt/disks/my-ssd/k9db/"

# Done.
echo "Experiment done"
echo "Terminated"
