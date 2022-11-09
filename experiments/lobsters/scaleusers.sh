#!/bin/bash
IP="10.128.0.22"
scales=("0.863" "1.73" "2.59" "3.45")
users=("5000" "10000" "15000" "20000")
loads=("725" "1000" "1000" "1000")

echo "Starting with pelton ..."
for index in "${!scales[@]}";
do
  scale=${scales[$index]}
  user=${users[$index]}
  load=${loads[$index]}
  echo "pelton-${user}"
  bazel run -c opt //:lobsters-harness -- \
    --runtime 120 \
    --datascale $scale \
    --reqscale $load \
    --queries pelton \
    --backend pelton \
    --prime \
    --scale_everything \
    "mysql://pelton:password@${IP}:10001/lobsters" > pelton-${user}.txt 2>&1
  mariadb -P10001 --host=${IP} -e "STOP";
  sleep 3
done

echo "Starting with baseline ..."
sleep 5
for index in "${!scales[@]}";
do
  scale=${scales[$index]}
  user=${users[$index]}
  load=${loads[$index]}
  echo "baseline-${user}"
  bazel run -c opt //:lobsters-harness -- \
    --runtime 120 \
    --datascale $scale \
    --reqscale $load \
    --queries pelton \
    --backend rocks-mariadb \
    --prime \
    --scale_everything \
    "mysql://pelton:password@${IP}:3306/lobsters" > base-${user}.txt 2>&1
  sleep 3
done

echo "Done all"
