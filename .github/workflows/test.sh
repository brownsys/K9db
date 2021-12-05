#!/bin/bash
./bin/drop.sh root password

# Run the proxy (in the background).
bazel run //mysql_proxy/src:mysql_proxy -- -logtostderr=1 &> .tmp &
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
