#!/bin/bash
./experiments/bin/drop_all.sh

# Compile everything with -c opt
bazel build -c opt //bin:mysql

bazel run -c opt //bin:mysql -- \
  --print=no --minloglevel=3 < experiments/GDPRbench/src/traces/mysql_$1.sql
