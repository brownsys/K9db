#!/bin/bash
rm -rf /tmp/k9db_*

if [[ "$1" == "dbg" ]]; then
  RUST_BACKTRACE=1 bazel run //:k9db -- --logtostderr=1 "${@:2}"

elif [[ "$1" == "opt" ]]; then
  rm -rf /mnt/disks/my-ssd/k9db/
  mkdir -p /mnt/disks/my-ssd/k9db/
  bazel run //:k9db --config opt -- --db_path=/mnt/disks/my-ssd/k9db/ "${@:2}"

elif [[ "$1" == "valgrind" ]]; then
  bazel run //:k9db --config valgrind -- --logtostderr=1 "${@:2}"

elif [[ "$1" == "asan" ]]; then
  LSAN_OPTIONS=suppressions=/home/bab/Documents/research/k9db/.lsan_jvm_suppress.txt \
  bazel run //:k9db --config asan -- --logtostderr=1

elif [[ "$1"  == "tsan" ]]; then
  LSAN_OPTIONS=suppressions=/home/bab/Documents/research/k9db/.lsan_jvm_suppress.txt \
  TSAN_OPTIONS=suppressions=/home/bab/Documents/research/k9db/.tsan_jvm_suppress.txt \
  bazel run //:k9db --config tsan -- --logtostderr=1

elif [[ "$1" == "unencrypted" ]]; then
  rm -rf /mnt/disks/my-ssd/k9db/
  mkdir -p /mnt/disks/my-ssd/k9db/
  bazel run //:k9db --config opt --encryption=off -- --db_path=/mnt/disks/my-ssd/k9db/ "${@:2}"

else
  echo 'use either "dbg", "opt", "unencrypted", "valgrind", "asan", or "tsan"!'
fi
