#!/bin/bash
# Compile everything with -c opt
bazel build -c opt //bin:mysql

bazel run -c opt //bin:mysql -- \
  --print=yes --logtostderr=yes < experiments/GDPRbench/src/traces/mysql.sql
