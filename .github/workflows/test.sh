#!/bin/bash
rm -rf /tmp/k9db_*

# Run each test file.
code=0
for path in "$@"
do
  # Run the proxy (in the background).
  bazel run //:k9db -- -logtostderr=1 &> .tmp &
  pid=$!
  sleep 10

  # Run one test file.
  mariadb --port=10001 --host=127.0.0.1 < "$path"
  status=$?
  if (( status != 0 )); then
    code=$status
  fi
  
  # Kill proxy.
  kill $pid
  wait $pid
done

# Display proxy output.
echo ""
echo ""
echo "Proxy output"
echo "================================"
cat .tmp
rm -rf .tmp

exit $code
