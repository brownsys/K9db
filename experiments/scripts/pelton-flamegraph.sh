#!/bin/bash
# Compile everything with -c opt
sudo bazel build -c opt //bin:cli

# Run cli with perf record.
sudo perf record -g -- \
  sudo bazel run -c opt //bin:cli -- \
    --print=no --db_path=:memory: < experiments/GDPRbench/src/traces/pelton.sql

# Format perf output and create the flamegraph.
sudo perf script > perf.script
/home/bab/.perf-flame-graph/stackcollapse-perf.pl perf.script \
  | /home/bab/.perf-flame-graph/flamegraph.pl > pelton.svg
