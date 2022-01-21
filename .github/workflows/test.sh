#!/bin/bash
rm -rf /tmp/pelton/rocksdb
mkdir -p /tmp/pelton/rocksdb

# Run the proxy (in the background).
bazel run //:pelton -- -logtostderr=1 &> .tmp &
pid=$!
sleep 10

# Run the test.
code=0
for path in "$@"
do
  mariadb --port=10001 --host=127.0.0.1 < "$path"
  status=$?
  if (( status != 0 )); then
    code=$status
  fi
done

# Kill the proxy.
kill $pid

# Display proxy output.
echo ""
echo ""
echo "Proxy output"
echo "================================"
cat .tmp
rm -rf .tmp

exit $code
