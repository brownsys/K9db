#!/bin/bash
# Compile everything with -c opt
sudo bazel build -c opt //bin:sqlite

# Run cli with perf record.
sudo perf record -g -- \
  sudo bazel run -c opt //bin:sqlite -- \
    --print=no --db_path=:memory: < experiments/GDPRbench/src/traces/sqlite.sql

# Format perf output and create the flamegraph.
sudo perf script > perf.script
/home/bab/.perf-flame-graph/stackcollapse-perf.pl perf.script \
  | /home/bab/.perf-flame-graph/flamegraph.pl > sqlite.svg
