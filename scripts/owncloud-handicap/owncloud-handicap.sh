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
ops=25000

# Go to owncloud directory
cd $PELTONDIR
cd experiments/ownCloud

# Pelton.
echo "Running pelton - no views"
bazel run :benchmark -c opt -- \
  --num-users $user \
  --users-per-group $groups \
  --files-per-user $files \
  --direct-shares-per-file $dshare \
  --group-shares-per-file $gshare \
  --in_size $insize \
  --operations $ops \
  --backend pelton \
  > "$OUT/pelton.out" 2>&1

# kill Pelton
echo "killing pelton"
mariadb -P10001 --host=127.0.0.1 -e "STOP"

# No views.
echo "Running pelton - no views"
bazel run :benchmark -c opt -- \
  --num-users $user \
  --users-per-group $groups \
  --files-per-user $files \
  --direct-shares-per-file $dshare \
  --group-shares-per-file $gshare \
  --in_size $insize \
  --operations $ops \
  --backend pelton \
  --accessors \
  > "$OUT/noviews.out" 2>&1

# kill Pelton
echo "killing pelton"
mariadb -P10001 --host=127.0.0.1 -e "STOP"

# Sleep
sleep 30

# No view + no indices
echo "Running pelton - no views + no indices"
bazel run :benchmark -c opt -- \
  --num-users $user \
  --users-per-group $groups \
  --files-per-user $files \
  --direct-shares-per-file $dshare \
  --group-shares-per-file $gshare \
  --in_size $insize \
  --operations $ops \
  --backend pelton \
  --views \
  --accessors \
  > "$OUT/noindices.out" 2>&1

# kill Pelton
echo "killing pelton"
mariadb -P10001 --host=127.0.0.1 -e "STOP"

# Sleep
sleep 30

# No view + no indices + no accessors
echo "Running pelton - no views + no indices + no accessors"
bazel run :benchmark -c opt -- \
  --num-users $user \
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
  > "$OUT/noaccessors.out" 2>&1

# kill Pelton
echo "killing pelton"
mariadb -P10001 --host=127.0.0.1 -e "STOP"

# Sleep until physical separated pelton can be compiled
sleep 90

# No view + no indices + no accessors
echo "Running pelton - physical separation + no views + no indices + no accessors"
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
  > "$OUT/physical.out" 2>&1

# kill Pelton
echo "killing pelton"
mariadb -P10001 --host=127.0.0.1 -e "STOP"

echo "All done!"
