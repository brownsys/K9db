#!/bin/bash
OUT="/home/pelton/pelton/scripts/lobsters"
echo "Writing output to $OUT"

RT=120
DS=1
RS=1000
TARGET_IP="$1"

cd ~/pelton/experiments/lobsters

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
