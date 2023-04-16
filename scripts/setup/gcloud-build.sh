#!/bin/bash
ROLE="$1"

# Run cargo raze.
mkdir -p /tmp/cargo-raze/doesnt/exist/
cd /home/pelton/pelton
cd pelton/proxy && cargo raze && cd -
cd experiments/lobsters && cargo raze && cd -
cd experiments/ownCloud && cargo raze && cd -
cd experiments/vote && cargo raze && cd -

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
