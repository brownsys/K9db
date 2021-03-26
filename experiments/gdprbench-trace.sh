#!/bin/bash
echo "Run this from pelton root directory!"
rm -rf /tmp/gdprbench && mkdir -p /tmp/gdprbench

# Run trace file!
GLOG_minloglevel=2 bazel run //bin:cli -c opt -- --print=no --db_path=:memory: < experiments/GDPRbench/src/traces/pelton.sql
