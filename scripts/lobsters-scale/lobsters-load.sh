#!/bin/bash
OUT="/home/pelton/pelton/scripts/lobsters-scale"
echo "Writing output to $OUT"

RT=120
DS=1
RS=1000
TARGET_IP="$1"

cd ~/pelton/experiments/lobsters

# Loop over datascales
for DS in "1" "2.5" "5" "7.5" "10"
do
  echo "Datascale $DS"
  bazel run -c opt //:lobsters-harness -- \
    --runtime $RT --datascale $DS --reqscale $RS --queries pelton \
    --backend pelton --prime --scale_everything \
    "mysql://root:password@$TARGET_IP:10001/lobsters" \
    > "$OUT/scale$DS.out" 2>&1

  # Stop pelton server.
  echo "Killing pelton server..."
  mariadb -P10001 --host=$TARGET_IP -e "STOP";

  # Sleep a while.
  sleep 20
done
