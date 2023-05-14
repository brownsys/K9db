#!/bin/bash
mkdir -p /mnt/disks/my-ssd/k9db

# sudo is not available inside docker container
SUDO="sudo "
if [[ "$(whoami)" == "root" ]]; then
  SUDO=""
fi

DB_IP=$LOCAL_IP
if [[ $LOCAL_IP == "" ]]; then
  DB_IP="127.0.0.1"
fi

echo "Check load instance for output"
echo "Running server on $DB_IP:10001"

# Build K9db in opt mode.
bazel build //:k9db --config=opt

# Restart mariadb.
# Load script will now run the baseline load.
echo "MariaDB Baseline"
$SUDO service mariadb restart

# wait for kill signal from load
rm -rf /mnt/disks/my-ssd/k9db/k9db
bazel run //:k9db --config=opt -- --hostname="$DB_IP:10001" --db_path="/mnt/disks/my-ssd/k9db/"

# Baseline load done.
$SUDO service mariadb stop

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
