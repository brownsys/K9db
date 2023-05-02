#!/bin/bash
# Stop mariadb.
sudo service mariadb stop

for DS in "1.72" "3.44" "4.31" "5.17" "6.89" "7.76" "8.62" "10.34" "12.9" "17.24"
do
  echo "Datascale $DS"
  rm -rf /mnt/disks/my-ssd/k9db/k9db
  bazel run //:k9db --config=opt -- --hostname="$LOCAL_IP:10001" --db_path="/mnt/disks/my-ssd/k9db/"
done


echo "Done!"
echo "Terminated"
