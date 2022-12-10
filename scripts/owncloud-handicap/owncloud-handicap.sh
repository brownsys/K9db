#!/bin/bash
PELTONDIR="/home/pelton/pelton"
OUT="$PELTONDIR/scripts/owncloud-handicap"
echo "Writing output to $OUT"

# Experiment parameters.
user=100000
groups=5
files=3
dshare=3
gshare=2
insize=50
ops=500

# Go to owncloud directory
cd $PELTONDIR
cd experiments/ownCloud

# No view + no indices + no accessors
echo "Running pelton - physical separation + no views + no indices + no accessors"
bazel run :benchmark -c opt -- \
  --num-users 10 \
  --users-per-group $groups \
  --files-per-user $files \
  --direct-shares-per-file $dshare \
  --group-shares-per-file $gshare \
  --in_size $insize \
  --operations $ops \
  --backend pelton \
  --views \
  --indices \
  --accessors \
  > "$OUT/physical10.out" 2>&1

# kill Pelton
echo "killing pelton"
mariadb -P10001 --host=127.0.0.1 -e "STOP"
  
bazel run :benchmark -c opt -- \
  --num-users 50 \
  --users-per-group $groups \
  --files-per-user $files \
  --direct-shares-per-file $dshare \
  --group-shares-per-file $gshare \
  --in_size $insize \
  --operations $ops \
  --backend pelton \
  --views \
  --indices \
  --accessors \
  > "$OUT/physical.out50" 2>&1
  
# kill Pelton
echo "killing pelton"
mariadb -P10001 --host=127.0.0.1 -e "STOP"
  
bazel run :benchmark -c opt -- \
  --num-users 100 \
  --users-per-group $groups \
  --files-per-user $files \
  --direct-shares-per-file $dshare \
  --group-shares-per-file $gshare \
  --in_size $insize \
  --operations $ops \
  --backend pelton \
  --views \
  --indices \
  --accessors \
  > "$OUT/physical.out100" 2>&1

# kill Pelton
echo "killing pelton"
mariadb -P10001 --host=127.0.0.1 -e "STOP"
  
bazel run :benchmark -c opt -- \
  --num-users 500 \
  --users-per-group $groups \
  --files-per-user $files \
  --direct-shares-per-file $dshare \
  --group-shares-per-file $gshare \
  --in_size $insize \
  --operations $ops \
  --backend pelton \
  --views \
  --indices \
  --accessors \
  > "$OUT/physical.out500" 2>&1
  
# kill Pelton
echo "killing pelton"
mariadb -P10001 --host=127.0.0.1 -e "STOP"
  
bazel run :benchmark -c opt -- \
  --num-users 1000 \
  --users-per-group $groups \
  --files-per-user $files \
  --direct-shares-per-file $dshare \
  --group-shares-per-file $gshare \
  --in_size $insize \
  --operations $ops \
  --backend pelton \
  --views \
  --indices \
  --accessors \
  > "$OUT/physical.out1000" 2>&1

# kill Pelton
echo "killing pelton"
mariadb -P10001 --host=127.0.0.1 -e "STOP"

echo "All done!"
