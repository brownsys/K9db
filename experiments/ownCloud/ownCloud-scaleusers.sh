#!/bin/bash
users=("100" "1000" "10000" "100000")
groups=5
files=3
dshare=3
gshare=2
writes=20
insize=50
ops=500

# Fresh new directory for results
rm -rf /home/pelton/owncloud
mkdir -p /home/pelton/owncloud

# Go to the pelton root directory.
cd ~/pelton
sudo service mariadb stop

echo "Starting pelton..."
for i in "${!users[@]}"; do
  user=${users[$i]}
  # Run proxy.
  echo "Running proxy $user"
  ./run.sh opt > /home/pelton/owncloud/proxy-$user 2>&1 &
  sleep 3

  echo "Running pelton harness $user"
  bazel run :benchmark -c opt -- \
  --num-users $user \
  --users-per-group $groups \
  --files-per-user $files \
  --direct-shares-per-file $dshare \
  --group-shares-per-file $gshare \
  --write_every $writes \
  --in_size $insize \
  --operations $ops \
  --backend pelton > /home/pelton/owncloud/pelton-$user 2>&1
  
  # kill Pelton
  echo "killing pelton"
  mariadb -P10001 --host=127.0.0.1 -e "STOP"
  sleep 3
done

echo "Starting baseline..."
sudo service mariadb start
sleep 3
for i in "${!users[@]}"; do
  user=${users[$i]}

  # run ownCloud harness
  echo "Running baseline harness $user"
  bazel run :benchmark -c opt -- \
  --num-users $user \
  --users-per-group $groups \
  --files-per-user $files \
  --direct-shares-per-file $dshare \
  --group-shares-per-file $gshare \
  --write_every $writes \
  --in_size $insize \
  --operations $ops \
  --backend mariadb > /home/pelton/owncloud/baseline-$user 2>&1

  sleep 3
done

echo "All done!"
