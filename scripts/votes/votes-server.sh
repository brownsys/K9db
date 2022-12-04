#!/bin/bash
PELTONDIR="/home/pelton/pelton"
OUT="$PELTONDIR/scripts/votes"
echo "Writing output to $OUT"
cd $PELTONDIR

# Run mariadb.
sudo service mariadb restart

# Run memcached.
cd experiments/memcached
bazel run @memcached//:memcached -c opt > "$OUT/memcached.log" 2>&1 &
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
