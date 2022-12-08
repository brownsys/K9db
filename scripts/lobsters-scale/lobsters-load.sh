#!/bin/bash
OUT="/home/pelton/pelton/scripts/lobsters-scale"
echo "Writing output to $OUT"

RT=120
DS=1
RS=1000
TARGET_IP="$1"

cd ~/pelton/experiments/lobsters

# Loop over datascales
#for DS in "1.72" "3.44" "5.17" "6.89" "8.62"        # 10k:50k:10k
for DS in "7.76" "10.34" "12.9" "8.62" "17.24"       # 45k, 60k, 75k, 50k, 100k
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
