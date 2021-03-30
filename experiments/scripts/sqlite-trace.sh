#!/bin/bash
# Compile everything with -c opt
bazel build -c opt //bin:sqlite

bazel run -c opt //bin:sqlite -- \
  --print=no --db_path=:memory: < experiments/GDPRbench/src/traces/sqlite.sql
