#!/bin/bash
PELTONDIR="/home/pelton/pelton"
OUT="$PELTONDIR/scripts/owncloud-comparison"
echo "Writing output to $OUT"
cd $PELTONDIR

# Run mariadb.
sudo service mariadb restart

# Run memcached.
cd experiments/memcached
bazel run @memcached//:memcached --config=opt -- -M -m 2048  > "$OUT/memcached.log" 2>&1 &
pid=$!

# Get signal to kill memcached.
cd $PELTONDIR
./run.sh opt
kill $pid

sudo service mariadb stop
wait $pid

# Run pelton actually.
./run.sh opt

echo "Terminated"
