#!/bin/bash
./bin/drop_all.sh

# Compile everything with -c opt
sudo bazel build -c opt //bin:mysql

# Run cli with perf record.
sudo perf record -g -- \
  sudo bazel run -c opt //bin:mysql -- \
    --print=no --minloglevel=3 < experiments/GDPRbench/src/traces/mysql.sql

# Format perf output and create the flamegraph.
sudo perf script > perf.script
/home/bab/.perf-flame-graph/stackcollapse-perf.pl perf.script \
  | /home/bab/.perf-flame-graph/flamegraph.pl > mysql.svg
