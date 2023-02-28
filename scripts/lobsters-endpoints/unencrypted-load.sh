#!/bin/bash
OUT="/home/pelton/pelton/scripts/lobsters-endpoints"
echo "Writing output to $OUT"

RT=120
#15k users
DS=2.59
RS=1000
TARGET_IP="$1"

cd ~/pelton/experiments/lobsters

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
