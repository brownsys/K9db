#!/bin/bash
OUT="/home/pelton/pelton/scripts/lobsters-throughput"
echo "Writing output to $OUT"

RT=120
DS=2.59
RS=1000
TARGET_IP="$1"

cd ~/pelton/experiments/lobsters

# Loop over datascales
for RS in "1000" "1100" "1200" "1300" "1400" "1500" "1750" "2000" "2250" "2500"
do
  echo "Datascale $DS"
  bazel run -c opt //:lobsters-harness -- \
    --runtime $RT --datascale $DS --reqscale $RS --queries pelton \
    --backend pelton --prime --scale_everything \
    "mysql://root:password@$TARGET_IP:10001/lobsters" \
    > "$OUT/throughput$RS.out" 2>&1

  # Stop pelton server.
  echo "Killing pelton server..."
  mariadb -P10001 --host=$TARGET_IP -e "STOP";

  # Sleep a while.
  sleep 20
done
