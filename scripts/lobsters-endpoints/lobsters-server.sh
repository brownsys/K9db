#!/bin/bash
PELTONDIR="/home/pelton/pelton"
OUT="$PELTONDIR/scripts/lobsters-endpoints"
echo "Writing output to $OUT"
cd $PELTONDIR

sudo service mariadb restart

# build memcached experiment
cd experiments/memcached
bazel build @memcached//:memcached --config=opt
bazel build //memcached:memcached --config=opt

# Wait for kill signal.
cd $PELTONDIR
./run.sh opt --hostname="$LOCAL_IP:10001"

# Memcached all queries
cd experiments/memcached
bazel run @memcached//:memcached --config=opt -- -m 1024 -M > "$OUT/memcached1.log" 2>&1 &
pid=$!
bazel run //memcached:memcached --config=opt --queries=all > "$OUT/memcached-all.out" 2>&1 &
kill $pid

# Memcached real queries
bazel run @memcached//:memcached --config=opt -- -m 1024 -M > "$OUT/memcached2.log" 2>&1 &
pid=$!
bazel run //memcached:memcached --config=opt --queries=real > "$OUT/memcached-real.out" 2>&1 &
kill $pid

# Done with memcached
cd $PELTONDIR
sudo service mariadb stop

# Run pelton
echo "Pelton (encrypted)"
./run.sh opt --hostname="$LOCAL_IP:10001"
echo "Done!"

# Run pelton (unencrypted)
echo "Pelton (unencrypted)"
./run.sh unencrypted --hostname="$LOCAL_IP:10001"
echo "Terminated"
