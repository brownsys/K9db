#!/bin/bash
./experiments/scripts/clear-db.sh

# Compile everything with -c opt
bazel build -c opt ...

bazel run //bin:cli -c opt -- \
  --print=no --minloglevel=3 < experiments/GDPRbench/src/traces/pelton.sql
