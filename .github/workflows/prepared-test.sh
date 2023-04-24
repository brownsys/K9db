#!/bin/bash
rm -rf /tmp/k9db_*

# Run the proxy (in the background).
bazel build //:k9db
bazel run //:k9db -- -logtostderr=1 &> .tmp &
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
