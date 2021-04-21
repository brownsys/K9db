#!/bin/bash
# Compile everything with -c opt
bazel build -c opt ...

bazel run //bin:cli -c opt -- \
  --print=yes --logtostderr=yes < experiments/GDPRbench/src/traces/pelton.sql
