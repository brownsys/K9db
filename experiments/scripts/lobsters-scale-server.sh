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

# Stop mariadb.
$SUDO service mariadb stop

for DS in "1.72" "3.44" "4.31" "5.17" "6.89" "7.76" "8.62" "10.34" "12.9" "17.24"
do
  echo "Datascale $DS"
  rm -rf /mnt/disks/my-ssd/k9db/k9db
  bazel run //:k9db --config=opt -- --hostname="$DB_IP:10001" --db_path="/mnt/disks/my-ssd/k9db/"
done


echo "Done!"
echo "Terminated"
