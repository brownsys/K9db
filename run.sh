#!/bin/bash
rm -rf /tmp/pelton
mkdir -p /tmp/pelton

if [[ "$1" == "dbg" ]]; then
  RUST_BACKTRACE=1 bazel run //:pelton -- --logtostderr=1 "${@:2}"

elif [[ "$1" == "opt" ]]; then
  bazel run //:pelton --config opt -- "${@:2}"

elif [[ "$1" == "valgrind" ]]; then
  bazel run --run_under "valgrind --error-exitcode=1 --errors-for-leak-kinds=definite --leak-check=full --show-leak-kinds=definite" //:pelton

elif [[ "$1" == "asan" ]]; then
  LSAN_OPTIONS=suppressions=/home/bab/Documents/research/pelton/.lsan_jvm_suppress.txt \
  bazel run //:pelton --config asan -- --logtostderr=1

elif [[ "$1"  == "tsan" ]]; then
  LSAN_OPTIONS=suppressions=/home/bab/Documents/research/pelton/.lsan_jvm_suppress.txt \
  TSAN_OPTIONS=suppressions=/home/bab/Documents/research/pelton/.tsan_jvm_suppress.txt \
  bazel run //:pelton --config tsan -- --logtostderr=1

else
  echo 'use either "dbg", "opt", "valgrind", "asan", or "tsan"!'
fi
