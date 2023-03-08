#!/bin/bash
OUT="/home/pelton/pelton/scripts/lobsters-throughput"
echo "Writing output to $OUT"

RT=120
DS=2.59
RS=1000
TARGET_IP="$1"

cd ~/pelton/experiments/lobsters

# Loop over datascales
for RS in "1000" "1250" "1500" "1750" "2000" "2500" "3000" "4000" "5000" "7500"
do
  echo "Datascale $DS"
  bazel run -c opt //:lobsters-harness -- \
    --runtime $RT --datascale $DS --reqscale $RS --queries pelton \
    --backend rocks-mariadb --prime --scale_everything \
    "mysql://root:password@$TARGET_IP:10001/lobsters" \
    > "$OUT/baseline$RS.out" 2>&1

  # Sleep a while.
  sleep 20
done

mariadb -P10001 --host=$TARGET_IP -e "STOP";
