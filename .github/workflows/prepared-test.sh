#!/bin/bash
rm -rf /tmp/pelton/rocksdb
mkdir -p /tmp/pelton/rocksdb

# Run the proxy (in the background).
bazel build //:pelton
bazel run //:pelton -- -logtostderr=1 &> .tmp &
pid=$!
sleep 10

# Run the test.
cd .github/workflows/
javac PreparedTest.java
java -cp ./mariadb-java-client-2.5.4.jar:. -enableassertions PreparedTest
code=$?
cd -

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
