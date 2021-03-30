#!/bin/bash
# Compile everything with -c opt
bazel build -c opt ...

bazel run //bin:cli -c opt -- \
  --print=no --db_path=:memory: < experiments/GDPRbench/src/traces/pelton.sql
