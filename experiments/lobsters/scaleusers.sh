#!/bin/bash
scales=("0.173", "0.863", "1.73", "2.59", "3.45",  "4.312")
users=("1000", "5000", "10000", "15000", "20000", "25000")
loads=("300", "725", "1000", "1000", "1000")

echo "Starting with pelton ..."
for index in "${!scales[@]}";
do
  scale=${scales[$index]}
  users=${users[$index]}
  load=${loads[$index]}
  echo "pelton-${users}"
  bazel run -c opt //:lobsters-harness -- \
    --runtime 120 \
    --datascale $scale \
    --reqscale $load \
    --queries pelton \
    --backend pelton \
    --prime \
    --scale_everything \
    "mysql://pelton:password@10.128.0.18:10001/lobsters" > pelton-${users}.txt 2>&1
  mariadb -P10001 --host=10.128.0.18 -e "STOP";
  sleep 3
done

echo "Starting with baseline ..."
sleep 5
for index in "${!scales[@]}";
do
  scale=${scales[$index]}
  users=${users[$index]}
  load=${loads[$index]}
  echo "baseline-${users}"
  bazel run -c opt //:lobsters-harness -- \
    --runtime 120 \
    --datascale $scale \
    --reqscale $load \
    --queries pelton \
    --backend rocks-mariadb \
    --prime \
    --scale_everything \
    "mysql://pelton:password@10.128.0.18:3306/lobsters" > base-${users}.txt 2>&1
  sleep 3
done
