#!/bin/bash
bazel run --config=opt //bin:mysql -- \
  --print=no < mysql.sql
