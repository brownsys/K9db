#!/bin/bash
user=5000
groups=5
files=3
dshare=3
gshare=2
writes=20
insize=50
ops=500
warmup=500

# Fresh new directory for results
rm -rf /home/pelton/owncloud-compare
mkdir -p /home/pelton/owncloud-compare

# Go to the pelton root directory.
sudo service mariadb stop

# Pelton
echo "Starting pelton..."
echo "Running proxy"
cd ~/pelton
./run.sh opt > /home/pelton/owncloud-compare/proxy 2>&1 &
sleep 3

echo "Running pelton harness"
cd experiments/ownCloud
bazel run :benchmark -c opt -- \
  --num-users $user \
  --users-per-group $groups \
  --files-per-user $files \
  --direct-shares-per-file $dshare \
  --group-shares-per-file $gshare \
  --write_every $writes \
  --in_size $insize \
  --operations $ops \
  --backend pelton > /home/pelton/owncloud-compare/pelton 2>&1
  
# kill Pelton
echo "killing pelton"
mariadb -P10001 --host=127.0.0.1 -e "STOP"
sleep 3

# Baseline next.
echo "Starting baseline..."
sudo service mariadb start
sleep 3

echo "Running baseline harness"
bazel run :benchmark -c opt -- \
  --num-users $user \
  --users-per-group $groups \
  --files-per-user $files \
  --direct-shares-per-file $dshare \
  --group-shares-per-file $gshare \
  --write_every $writes \
  --in_size $insize \
  --operations $ops \
  --backend mariadb > /home/pelton/owncloud-compare/baseline 2>&1
sleep 3

# Memcached now.
echo "Starting memcached..."
cd ../memcached

echo "Running memcached"
bazel run @memcached//:memcached -c opt > /home/pelton/owncloud-compare/memlog &
pid=$!

echo "Running memcached harness"
bazel run :benchmark -c opt -- \
  --num-users $user \
  --users-per-group $groups \
  --files-per-user $files \
  --direct-shares-per-file $dshare \
  --group-shares-per-file $gshare \
  --write_every $writes \
  --in_size $insize \
  --operations $ops \
  --warmup $warmup \
  --backend memcached > /home/pelton/owncloud-compare/baseline 2>&1

kill $pid
sleep 3

echo "All done!"
