#!/bin/bash
ROLE="$1"

# Build ownCloud harness
cd ~/pelton/experiments/ownCloud
bazel build ... -c opt

# Build vote harness
cd ~/pelton/experiments/vote
bazel build ... -c opt

# Build pelton
cd ~/pelton
if [[ "$ROLE" == "db" ]]; then
  bazel build //:pelton --config opt
else
  cd experiments/lobsters
  bazel build ... -c opt
fi
