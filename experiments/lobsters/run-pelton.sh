#!/bin/bash
bazel run --config=opt //bin:cli -- \
  --db_name=peltonlobsters --print=no --minloglevel=3 < pelton.sql
