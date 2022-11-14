#!/bin/bash
OUT="$pwd/lobsters/"
echo "Writing output to $OUT"

RT=30
DS=0.01
RS=50

# Baseline
echo "Priming baseline..."
bazel run -c opt //:lobsters-harness -- \
  --runtime 0 --datascale $DS --reqscale $RS --queries pelton \
  --backend rocks-mariadb --prime --scale_everything \
  "mysql://pelton:password@$TARGET_IP:3306/lobsters" \
  > "$OUT/base-prime.txt" 2>&1

echo "Running baseline load..."
bazel run -c opt //:lobsters-harness -- \
  --runtime $RT --datascale $DS --reqscale $RS --queries pelton \
  --backend rocks-mariadb --scale_everything \
  "mysql://pelton:password@$TARGET_IP:3306/lobsters" \
  > "$OUT/baseline.txt"  2>&1 
  
# Pelton.
echo "Priming pelton..."
bazel run -c opt //:lobsters-harness -- \
  --runtime 0 --datascale $DS --reqscale $RS --queries pelton \
  --backend pelton --prime --scale_everything \
  "mysql://root:password@$TARGET_IP:10001/lobsters" \
  > "$OUT/pelton-prime.txt" 2>&1

echo "Running pelton load..."
bazel run -c opt //:lobsters-harness -- \
  --runtime $RT --datascale $DS --reqscale $RS --queries pelton \
  --backend pelton --scale_everything \
  "mysql://root:password@$TARGET_IP:10001/lobsters" \
  > "$OUT/pelton.txt" 2>&1

# Stop pelton server.
echo "Killing pelton server..."
mariadb -P10001 --host=${IP} -e "STOP";
