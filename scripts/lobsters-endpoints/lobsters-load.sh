#!/bin/bash
OUT="/home/pelton/pelton/scripts/lobsters-endpoints"
echo "Writing output to $OUT"

RT=120
#15k users
DS=2.59
RS=1000
TARGET_IP="$1"

cd ~/pelton/experiments/lobsters

# Baseline
echo "Priming baseline..."
bazel run -c opt //:lobsters-harness -- \
  --runtime 0 --datascale $DS --reqscale $RS --queries pelton \
  --backend rocks-mariadb --prime --scale_everything \
  "mysql://pelton:password@$TARGET_IP:3306/lobsters" \
  > "$OUT/base-prime.out" 2>&1

echo "Running baseline load..."
bazel run -c opt //:lobsters-harness -- \
  --runtime $RT --datascale $DS --reqscale $RS --queries pelton \
  --backend rocks-mariadb --scale_everything \
  "mysql://pelton:password@$TARGET_IP:3306/lobsters" \
  > "$OUT/baseline.out" 2>&1

# Show DB size of baseline.
mariadb -P3306 --host=$TARGET_IP -u pelton -ppassword \
  -e "SELECT table_schema AS 'Database', SUM(data_length + index_length) / 1024 / 1024 AS 'Size (MB)' FROM information_schema.TABLES GROUP BY table_schema"

# Send signal to server to kill mariadb and start pelton.
echo "Killing pelton server..."
mariadb -P10001 --host=$TARGET_IP -e "STOP";
sleep 600  # Sleep long enough for memcached experiment to run and build on server.

# Pelton.
echo "Priming pelton..."
bazel run -c opt //:lobsters-harness -- \
  --runtime 0 --datascale $DS --reqscale $RS --queries pelton \
  --backend pelton --prime --scale_everything \
  "mysql://root:password@$TARGET_IP:10001/lobsters" \
  > "$OUT/pelton-prime.out" 2>&1

echo "Running pelton load..."
bazel run -c opt //:lobsters-harness -- \
  --runtime $RT --datascale $DS --reqscale $RS --queries pelton \
  --backend pelton --scale_everything \
  "mysql://root:password@$TARGET_IP:10001/lobsters" \
  > "$OUT/pelton.out" 2>&1

echo "Finding memory..."
mariadb -P10001 --host=$TARGET_IP -e "SHOW MEMORY" > "$OUT/memory.out" 2>&1

# Stop pelton server.
echo "Killing pelton server..."
mariadb -P10001 --host=$TARGET_IP -e "STOP";

sleep 240

# Pelton unencrypted.
echo "Priming pelton (unencrypted)..."
bazel run -c opt //:lobsters-harness -- \
  --runtime 0 --datascale $DS --reqscale $RS --queries pelton \
  --backend pelton --prime --scale_everything \
  "mysql://root:password@$TARGET_IP:10001/lobsters" \
  > "$OUT/unencrypted-prime.out" 2>&1

echo "Running pelton load (unencrypted)..."
bazel run -c opt //:lobsters-harness -- \
  --runtime $RT --datascale $DS --reqscale $RS --queries pelton \
  --backend pelton --scale_everything \
  "mysql://root:password@$TARGET_IP:10001/lobsters" \
  > "$OUT/unencrypted.out" 2>&1

# Stop pelton server.
echo "Killing pelton server..."
mariadb -P10001 --host=$TARGET_IP -e "STOP";
