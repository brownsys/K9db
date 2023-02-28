#!/bin/bash
ROLE="$1"

# Build ownCloud harness
cd /home/pelton/pelton/experiments/ownCloud
bazel build ... -c opt

# Build vote harness
cd /home/pelton/pelton/experiments/vote
bazel build ... -c opt

# Build memcached
cd /home/pelton/pelton/experiments/memcached
bazel build @memcached//:memcached --config=opt

# Build pelton
cd /home/pelton/pelton
if [[ "$ROLE" == "db" ]]; then
  bazel build ... --config opt
else
  cd experiments/lobsters
  bazel build ... -c opt
fi
