#!/bin/bash
./bin/drop.sh root password

# Build memcached.
bazel build @memcached//:memcached

# Run memcached (in the background).
bazel run @memcached//:memcached -- -vv -u memcached &> .memtmp &
pid=$!
sleep 10

# Run the test.
bazel test //baseline/... --test_output=all
code=$?

# Kill memcached.
kill $pid

# Display memcached output.
echo ""
echo ""
echo "Memcached output"
echo "================================"
cat .memtmp
rm -rf .memtmp

exit $code