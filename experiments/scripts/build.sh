#!/bin/bash

# Our current location is the root dir of the repo.
K9DB_DIR="$(pwd)"

# Build ownCloud harness
echo "Building ownCloud harness..."
cd ${K9DB_DIR}/experiments/ownCloud
bazel build //:benchmark -c opt

# Build vote harness
echo "Building votes harness..."
cd ${K9DB_DIR}/experiments/vote
bazel build //:vote-benchmark -c opt

# Build memcached
echo "Building memcached..."
cd ${K9DB_DIR}/experiments/memcached
bazel build @memcached//:memcached --config=opt
bazel build //memcached:memcached --config=opt --queries=real

# Build lobsters harness
echo "Building lobsters harness..."
cd ${K9DB_DIR}/experiments/lobsters
bazel build //:lobsters-harness -c opt

# Build K9db.
echo "Building K9db (will take a while)..."
cd ${K9DB_DIR}
bazel build //:k9db --config=opt
