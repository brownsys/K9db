#!/bin/bash
PELTONDIR="/home/pelton/pelton"
PELTONDIR="../.."
OUT="$PELTONDIR/scripts/owncloud-comparison"
echo "Writing output to $OUT"
cd $PELTONDIR

# Run mariadb.
sudo service mariadb restart

# Run memcached.
cd experiments/memcached
bazel run @memcached//:memcached --config=opt -- -U 0 -p 11211 -M -m 2048 > "$OUT/memcached.log" 2>&1 &
pid=$!

# Get signal to kill memcached.
cd $PELTONDIR
./run.sh opt

# Find memory usage in memcached
echo "stats" | nc localhost 11211 -q 1 > "$OUT/memcached-memory.out" 2>&1
kill $pid

sudo service mariadb stop
wait $pid

# Run pelton actually.
./run.sh opt

echo "Terminated"
