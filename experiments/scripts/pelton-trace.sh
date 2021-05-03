#!/bin/bash
if [[ "$1" == "" ]]; then
  echo "insert"
  ./experiments/scripts/pelton-trace.sh insert 2>>err.log | grep "Perf start" -A 15
  echo ""
  echo ""
  echo "++++++++++++++++++++++++++++++++++++++++++++++++++"
  echo ""
  echo ""

  echo "select_pk"
  ./experiments/scripts/pelton-trace.sh select_pk 2>>err.log | grep "Perf start" -A 15
  echo ""
  echo ""
  echo "++++++++++++++++++++++++++++++++++++++++++++++++++"
  echo ""
  echo ""

  echo "select_usr"
  ./experiments/scripts/pelton-trace.sh select_usr 2>>err.log | grep "Perf start" -A 15
  echo ""
  echo ""
  echo "++++++++++++++++++++++++++++++++++++++++++++++++++"
  echo ""
  echo ""

  echo "select_pur"
  ./experiments/scripts/pelton-trace.sh select_pur 2>>err.log | grep "Perf start" -A 15
  echo ""
  echo ""
  echo "++++++++++++++++++++++++++++++++++++++++++++++++++"
  echo ""
  echo ""

  echo "scan"
  ./experiments/scripts/pelton-trace.sh scan 2>>err.log | grep "Perf start" -A 15
  echo ""
  echo ""
  echo "++++++++++++++++++++++++++++++++++++++++++++++++++"
  echo ""
  echo ""

  echo "delete_pk"
  ./experiments/scripts/pelton-trace.sh delete_pk 2>>err.log | grep "Perf start" -A 15
  echo ""
  echo ""
  echo "++++++++++++++++++++++++++++++++++++++++++++++++++"
  echo ""
  echo ""

  echo "delete_usr"
  ./experiments/scripts/pelton-trace.sh delete_usr 2>>err.log | grep "Perf start" -A 15
  echo ""
  echo ""
  echo "++++++++++++++++++++++++++++++++++++++++++++++++++"
  echo ""
  echo ""

  echo "delete_pur"
  ./experiments/scripts/pelton-trace.sh delete_pur 2>>err.log | grep "Perf start" -A 15
  echo ""
  echo ""
  echo "++++++++++++++++++++++++++++++++++++++++++++++++++"
  echo ""
  echo ""

  echo "update_pk"
  ./experiments/scripts/pelton-trace.sh update_pk 2>>err.log | grep "Perf start" -A 15
  echo ""
  echo ""
  echo "++++++++++++++++++++++++++++++++++++++++++++++++++"
  echo ""
  echo ""

  echo "update_usr"
  ./experiments/scripts/pelton-trace.sh update_usr 2>>err.log | grep "Perf start" -A 15
  echo ""
  echo ""
  echo "++++++++++++++++++++++++++++++++++++++++++++++++++"
  echo ""
  echo ""

  echo "update_pur"
  ./experiments/scripts/pelton-trace.sh update_pur 2>>err.log | grep "Perf start" -A 15
  echo ""
  echo ""
  echo "++++++++++++++++++++++++++++++++++++++++++++++++++"
  echo ""
  echo ""

else
  ./bin/drop.sh root password

  # Compile everything with -c opt
  bazel build --config=opt ...

  bazel run //bin:cli --config=opt -- \
    --print=no --minloglevel=3 < experiments/GDPRbench/src/traces/pelton_$1.sql
fi
