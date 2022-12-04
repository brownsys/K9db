#!/bin/bash
OUT="/home/pelton/pelton/scripts/lobsters-endpoints"
echo "Writing output to $OUT"

RT=120
DS=2.5
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

# Pelton.
echo "Priming pelton..."
bazel run -c opt //:lobsters-harness -- \
  --runtime 0 --datascale $DS --reqscale $RS --queries pelton \
  --backend pelton --prime --scale_everything \
  "mysql://root:password@$TARGET_IP:10001/lobsters" \
  > "$OUT/pelton-prime.out" 2>&1

mariadb -P10001 --host=$TARGET_IP -e "SET SERIALIAZBLE";

echo "Running pelton load..."
bazel run -c opt //:lobsters-harness -- \
  --runtime $RT --datascale $DS --reqscale $RS --queries pelton \
  --backend pelton --scale_everything \
  "mysql://root:password@$TARGET_IP:10001/lobsters" \
  > "$OUT/pelton.out" 2>&1

# Stop pelton server.
echo "Killing pelton server..."
mariadb -P10001 --host=$TARGET_IP -e "STOP";
