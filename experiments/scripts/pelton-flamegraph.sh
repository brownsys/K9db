#!/bin/bash
./bin/drop_all.sh

# Compile everything with -c opt
sudo bazel build --config=opt //bin:cli

# Run cli with perf record.
sudo perf record -g -- \
  sudo bazel run --config=opt //bin:cli -- \
    --print=no --minloglevel=3 < experiments/GDPRbench/src/traces/pelton.sql

# Format perf output and create the flamegraph.
sudo perf script > perf.script
/home/bab/.perf-flame-graph/stackcollapse-perf.pl perf.script \
  | /home/bab/.perf-flame-graph/flamegraph.pl > pelton.svg
