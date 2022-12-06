#!/bin/bash
PELTONDIR="/home/pelton/pelton"
OUT="$PELTONDIR/scripts/votes"
echo "Writing output to $OUT"

# Experiment parameters.
articles=10000
distribution="skewed"
target=10000
write=19
runtime=60
warmup=60
prime=30

# Go to owncloud directory
cd $PELTONDIR
cd experiments/vote

# Baseline mariadb.
echo "Running baseline harness"
echo "Priming"
bazel run :vote-benchmark -c opt -- \
  --articles $articles \
  -d $distribution \
  --target $target \
  --write-every 1 \
  --runtime $prime \
  mysql --address "$LOCAL_IP:3306" > $OUT/baseline-prime.out 2>&1
echo "Load"
bazel run :vote-benchmark -c opt -- \
  --articles $articles \
  -d $distribution \
  --target $target \
  --write-every $write \
  --runtime $runtime \
  --no-prime \
  mysql --address "$LOCAL_IP:3306"  > $OUT/baseline.out 2>&1
sleep 3

# Baseline + memcached.
echo "Running mariadb+memcached harness (priming and warmup)"
echo "Priming"
bazel run :vote-benchmark -c opt -- \
  --articles $articles \
  -d $distribution \
  --target $target \
  --write-every 1 \
  --runtime $prime \
  memcached-hybrid --mysql-address "$LOCAL_IP:3306" > $OUT/hybrid-prime.out 2>&1
echo "Warmup...."
bazel run :vote-benchmark -c opt -- \
  --articles $articles \
  -d $distribution \
  --target $target \
  --write-every $write \
  --runtime $warmup \
  --no-prime \
  memcached-hybrid --mysql-address "$LOCAL_IP:3306" > $OUT/hybrid-warmup.out 2>&1
echo "Load"
bazel run :vote-benchmark -c opt -- \
  --articles $articles \
  -d $distribution \
  --target $target \
  --write-every $write \
  --runtime $runtime \
  --no-prime \
  memcached-hybrid --mysql-address "$LOCAL_IP:3306"  > $OUT/hybrid.out 2>&1

# Kill memcached and mariadb
echo "Killing memcached & mariadb"
mariadb -P10001 --host=127.0.0.1 -e "STOP"
sleep 15

# Pelton!
echo "Running pelton harness"
echo "Priming"
bazel run :vote-benchmark -c opt -- \
  --articles $articles \
  -d $distribution \
  --target $target \
  --write-every 1 \
  --runtime $prime \
  pelton > $OUT/pelton-prime.out 2>&1
echo "Load"
bazel run :vote-benchmark -c opt -- \
  --articles $articles \
  -d $distribution \
  --target $target \
  --write-every $write \
  --runtime $runtime \
  --no-prime \
  pelton > $OUT/pelton.out 2>&1
sleep 3

# kill Pelton
echo "killing pelton"
mariadb -P10001 --host=127.0.0.1 -e "STOP"

echo "All done!"
