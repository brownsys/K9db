#!/bin/bash
PELTONDIR="/home/pelton/pelton"
OUT="$PELTONDIR/scripts/owncloud-handicap"
echo "Writing output to $OUT"

# Experiment parameters.
physical_user=1000
user=10000
groups=5
files=3
dshare=3
gshare=2
write_batch_size=1
read_in_size=10
write_every=19
ops=10000
zipf=0.6


# Go to owncloud directory
cd $PELTONDIR
cd experiments/ownCloud

# No view + no indices + no accessors
echo "Running pelton - no views"
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
  --views \
  > "$OUT/noviews.out" 2>&1

# kill Pelton
echo "killing pelton"
mariadb -P10001 --host=127.0.0.1 -e "STOP"

echo "Running pelton - no views + no accessors"
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
  --views \
  --accessors \
  > "$OUT/noaccessors.out" 2>&1

# kill Pelton
echo "killing pelton"
mariadb -P10001 --host=127.0.0.1 -e "STOP"

echo "Running pelton - physical separation + no views + no accessors"
bazel run :benchmark -c opt -- \
  --num-users $physical_user \
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
  --views \
  --indices \
  --accessors \
  > "$OUT/physical.out" 2>&1

# kill Pelton
echo "killing pelton"
mariadb -P10001 --host=127.0.0.1 -e "STOP"

echo "All done!"
