#!/bin/bash
PELTONDIR="/home/pelton/pelton"
OUT="$PELTONDIR/scripts/owncloud-comparison"
echo "Writing output to $OUT"

# Experiment parameters.
user=10000
groups=5
files=3
dshare=3
gshare=2
write_batch_size=20
read_in_size=20
write_every=19
ops=10000
zipf=0.6

# Go to owncloud directory
cd $PELTONDIR
cd experiments/ownCloud

# Baseline mariadb.
echo "Running baseline harness"
bazel run :benchmark -c opt -- \
  --num-users $user \
  --users-per-group $groups \
  --files-per-user $files \
  --direct-shares-per-file $dshare \
  --group-shares-per-file $gshare \
  --read_in_size $read_in_size \
  --write_batch_size $write_batch_size \
  --write_every $write_every \
  --operations $ops \
  --zipf $zipf \
  --backend mariadb \
  > "$OUT/baseline.out" 2>&1
sleep 3

# Show DB size of baseline.
mariadb -P3306 --host=$LOCAL_IP -u pelton -ppassword \
  -e "SELECT table_schema AS 'Database', SUM(data_length + index_length) / 1024 / 1024 AS 'Size (MB)' FROM information_schema.TABLES GROUP BY table_schema" \
  > "$OUT/mariadb-memory.out" 2>&1

# Memcached now.
echo "Running memcached harness"
bazel run :benchmark -c opt -- \
  --num-users $user \
  --users-per-group $groups \
  --files-per-user $files \
  --direct-shares-per-file $dshare \
  --group-shares-per-file $gshare \
  --read_in_size $read_in_size \
  --write_batch_size $write_batch_size \
  --write_every $write_every \
  --operations $ops \
  --warmup \
  --zipf $zipf \
  --backend memcached \
  > "$OUT/hybrid.out" 2>&1
sleep 3

echo "Killing memcached & mariadb"
mariadb -P10001 --host=127.0.0.1 -e "STOP"
sleep 15

# Pelton!
echo "Running pelton harness"
bazel run :benchmark -c opt -- \
  --num-users $user \
  --users-per-group $groups \
  --files-per-user $files \
  --direct-shares-per-file $dshare \
  --group-shares-per-file $gshare \
  --read_in_size $read_in_size \
  --write_batch_size $write_batch_size \
  --write_every $write_every \
  --operations $ops \
  --zipf $zipf \
  --backend pelton \
  > "$OUT/pelton.out" 2>&1
sleep 3

mariadb -P10001 --host=127.0.0.1 -e "SHOW MEMORY" > "$OUT/pelton-memory.out" 2>&1

# kill Pelton
echo "killing pelton"
mariadb -P10001 --host=127.0.0.1 -e "STOP"

echo "All done!"
