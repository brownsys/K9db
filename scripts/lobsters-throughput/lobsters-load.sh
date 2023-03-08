#!/bin/bash
OUT="/home/pelton/pelton/scripts/lobsters-throughput"
echo "Writing output to $OUT"

RT=120
DS=2.59
TARGET_IP="$1"

cd ~/pelton/experiments/lobsters

# Same but for baseline!
for RS in "1500" "2000" "2500" "3000" "3500" "4000" "4500"ss "5000" "6000"
do
  echo "Datascale $DS"
  bazel run -c opt //:lobsters-harness -- \
    --runtime $RT --datascale $DS --reqscale $RS --queries pelton \
    --backend rocks-mariadb --prime --scale_everything \
    "mysql://pelton:password@$TARGET_IP:3306/lobsters" \
    > "$OUT/baseline$RS.out" 2>&1

  # Sleep a while.
  sleep 20
done

mariadb -P10001 --host=$TARGET_IP -e "STOP";

# Loop over datascales
for RS in "4800" "4900"
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
