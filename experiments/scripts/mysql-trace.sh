#!/bin/bash
if [[ "$1" == "" ]]; then
  echo "insert"
  ./experiments/scripts/mysql-trace.sh insert 2>>err.log | grep "Vanilla MySql" -A 5
  echo ""
  echo ""
  echo "++++++++++++++++++++++++++++++++++++++++++++++++++"
  echo ""
  echo ""

  echo "select_pk"
  ./experiments/scripts/mysql-trace.sh select_pk 2>>err.log | grep "Vanilla MySql" -A 5
  echo ""
  echo ""
  echo "++++++++++++++++++++++++++++++++++++++++++++++++++"
  echo ""
  echo ""

  echo "select_usr"
  ./experiments/scripts/mysql-trace.sh select_usr 2>>err.log | grep "Vanilla MySql" -A 5
  echo ""
  echo ""
  echo "++++++++++++++++++++++++++++++++++++++++++++++++++"
  echo ""
  echo ""

  echo "select_pur"
  ./experiments/scripts/mysql-trace.sh select_pur 2>>err.log | grep "Vanilla MySql" -A 5
  echo ""
  echo ""
  echo "++++++++++++++++++++++++++++++++++++++++++++++++++"
  echo ""
  echo ""

  echo "scan"
  ./experiments/scripts/mysql-trace.sh scan 2>>err.log | grep "Vanilla MySql" -A 5
  echo ""
  echo ""
  echo "++++++++++++++++++++++++++++++++++++++++++++++++++"
  echo ""
  echo ""

  echo "delete_pk"
  ./experiments/scripts/mysql-trace.sh delete_pk 2>>err.log | grep "Vanilla MySql" -A 5
  echo ""
  echo ""
  echo "++++++++++++++++++++++++++++++++++++++++++++++++++"
  echo ""
  echo ""

  echo "delete_usr"
  ./experiments/scripts/mysql-trace.sh delete_usr 2>>err.log | grep "Vanilla MySql" -A 5
  echo ""
  echo ""
  echo "++++++++++++++++++++++++++++++++++++++++++++++++++"
  echo ""
  echo ""

  echo "delete_pur"
  ./experiments/scripts/mysql-trace.sh delete_pur 2>>err.log | grep "Vanilla MySql" -A 5
  echo ""
  echo ""
  echo "++++++++++++++++++++++++++++++++++++++++++++++++++"
  echo ""
  echo ""

  echo "update_pk"
  ./experiments/scripts/mysql-trace.sh update_pk 2>>err.log | grep "Vanilla MySql" -A 5
  echo ""
  echo ""
  echo "++++++++++++++++++++++++++++++++++++++++++++++++++"
  echo ""
  echo ""

  echo "update_usr"
  ./experiments/scripts/mysql-trace.sh update_usr 2>>err.log | grep "Vanilla MySql" -A 5
  echo ""
  echo ""
  echo "++++++++++++++++++++++++++++++++++++++++++++++++++"
  echo ""
  echo ""

  echo "update_pur"
  ./experiments/scripts/mysql-trace.sh update_pur 2>>err.log | grep "Vanilla MySql" -A 5
  echo ""
  echo ""
  echo "++++++++++++++++++++++++++++++++++++++++++++++++++"
  echo ""
  echo ""

else
  ./bin/drop.sh root password 2>>err.log | grep -v "Using a password on the command line"

  # Compile everything with -c opt
  bazel build -c opt //bin:mysql

  bazel run -c opt //bin:mysql -- \
    --print=no --minloglevel=3 < experiments/GDPRbench/src/traces/mysql_$1.sql
fi
