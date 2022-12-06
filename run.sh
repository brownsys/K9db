#!/bin/bash
rm -rf /tmp/pelton/rocksdb
mkdir -p /tmp/pelton/rocksdb

if [[ "$1" == "dbg" ]]; then
  RUST_BACKTRACE=1 bazel run //:pelton -- --logtostderr=1 "${@:2}"

elif [[ "$1" == "opt" ]]; then
  rm -rf /mnt/disks/my-ssd/pelton/
  mkdir -p /mnt/disks/my-ssd/pelton/
  bazel run //:pelton --config opt -- "${@:2}"

elif [[ "$1" == "valgrind" ]]; then
  bazel run //:pelton --config valgrind -- --logtostderr=1 "${@:2}"

elif [[ "$1" == "asan" ]]; then
  LSAN_OPTIONS=suppressions=/home/bab/Documents/research/pelton/.lsan_jvm_suppress.txt \
  bazel run //:pelton --config asan -- --logtostderr=1

elif [[ "$1"  == "tsan" ]]; then
  LSAN_OPTIONS=suppressions=/home/bab/Documents/research/pelton/.lsan_jvm_suppress.txt \
  TSAN_OPTIONS=suppressions=/home/bab/Documents/research/pelton/.tsan_jvm_suppress.txt \
  bazel run //:pelton --config tsan -- --logtostderr=1

elif [[ "$1" == "unencrypted" ]]; then
    rm -rf /mnt/disks/my-ssd/pelton/
  mkdir -p /mnt/disks/my-ssd/pelton/
  bazel run //:pelton --config opt --encryption=off -- "${@:2}"

else
  echo 'use either "dbg", "opt", "unencrypted", "valgrind", "asan", or "tsan"!'
fi
