#!/bin/bash
if [[ "$1" == "dbg" ]]; then
  ./bin/drop.sh root password
  bazel run //mysql_proxy/src:mysql_proxy -- --logtostderr=1

elif [[ "$1" == "opt" ]]; then
  ./bin/drop.sh root password
  bazel run //mysql_proxy/src:mysql_proxy --config opt

elif [[ "$1" == "asan" ]]; then
  ./bin/drop.sh root password

  LSAN_OPTIONS=suppressions=/home/bab/Documents/research/pelton/.lsan_jvm_suppress.txt \
  bazel run //mysql_proxy/src:mysql_proxy --config asan -- --logtostderr=1

elif [[ "$1"  == "tsan" ]]; then
  ./bin/drop.sh root password

  LSAN_OPTIONS=suppressions=/home/bab/Documents/research/pelton/.lsan_jvm_suppress.txt \
  TSAN_OPTIONS=suppressions=/home/bab/Documents/research/pelton/.tsan_jvm_suppress.txt \
  bazel run //mysql_proxy/src:mysql_proxy --config tsan -- --logtostderr=1

else
  echo 'use either "dbg", "opt", "asan", or "tsan"!'
fi
