# #!/bin/bash
bazel build --config valgrind //:pelton

rm -rf /tmp/pelton/rocksdb
mkdir -p /tmp/pelton/rocksdb

# E2E test for concurrent creates
echo "==================================================="
echo "==================================================="
echo "[test_multithreaded_proxy.sh]: Test: parallel_create"
echo "[test_multithreaded_proxy.sh]: Running proxy..."
echo "==================================================="
echo "==================================================="
bazel run --config valgrind //:pelton -- --logtostderr=1 &
proxy_pid=$!
sleep 15

echo "==================================================="
echo "==================================================="
echo "[test_multithreaded_proxy.sh]: Executing test..."
echo "==================================================="
echo "==================================================="
mariadb --port=10001 --host=127.0.0.1 < bin/sync_testing/parallel_create1.sql &
client_pid=$!
mariadb --port=10001 --host=127.0.0.1 < bin/sync_testing/parallel_create2.sql
code2=$?
wait $client_pid
code1=$?

echo "==================================================="
echo "==================================================="
echo "[test_multithreaded_proxy.sh]: Killing proxy..."
echo "==================================================="
echo "==================================================="
kill -s SIGTERM $proxy_pid
wait $proxy_pid
code_proxy=$?

echo "==================================================="
echo "==================================================="
if [ "$code1" != "0" ]; then
  echo "[test_multithreaded_proxy.sh]: Client 1 error"
  exit $code1
elif [ "$code2" != "0" ]; then
  echo "[test_multithreaded_proxy.sh]: Client 2 error"
  exit $code2
elif [ "$code_proxy" != "0" ]; then
  echo "[test_multithreaded_proxy.sh]: Proxy error"
  exit $code_proxy
fi
echo "[test_multithreaded_proxy.sh]: Passed!"
echo "..................................................."
echo "..................................................."

rm -rf /tmp/pelton/rocksdb
mkdir -p /tmp/pelton/rocksdb


# E2E test for concurrent inserts, deletes, and selects
echo "==================================================="
echo "==================================================="
echo "[test_multithreaded_proxy.sh]: Test: parallel operations"
echo "[test_multithreaded_proxy.sh]: Running proxy..."
echo "==================================================="
echo "==================================================="
bazel run --config valgrind //:pelton -- --logtostderr=1 &
proxy_pid=$!
sleep 15

echo "==================================================="
echo "==================================================="
echo "[test_multithreaded_proxy.sh]: Creating schema..."
echo "==================================================="
echo "==================================================="
mariadb --port=10001 --host=127.0.0.1 < bin/sync_testing/parallel_operations_schema.sql
code_schema=$?

echo "==================================================="
echo "==================================================="
echo "[test_multithreaded_proxy.sh]: Priming test..."
echo "==================================================="
echo "==================================================="
mariadb --port=10001 --host=127.0.0.1 < bin/sync_testing/parallel_operations_prime1.sql &
client_pid=$!
mariadb --port=10001 --host=127.0.0.1 < bin/sync_testing/parallel_operations_prime2.sql
code_prime_2=$?
wait $client_pid
code_prime_1=$?

echo "==================================================="
echo "==================================================="
echo "[test_multithreaded_proxy.sh]: Executing test..."
echo "==================================================="
echo "==================================================="
mariadb --port=10001 --host=127.0.0.1 < bin/sync_testing/parallel_operations1.sql 1 > /dev/null &
client_pid=$!
mariadb --port=10001 --host=127.0.0.1 < bin/sync_testing/parallel_operations2.sql 1 > /dev/null
code2=$?
wait $client_pid
code1=$?

echo "==================================================="
echo "==================================================="
echo "[test_multithreaded_proxy.sh]: Killing proxy..."
echo "==================================================="
echo "==================================================="
kill -s SIGTERM $proxy_pid
wait $proxy_pid
code_proxy=$?

echo "==================================================="
echo "==================================================="
if [ "$code_schema" != "0" ]; then
  echo "[test_multithreaded_proxy.sh]: Schema error"
  exit $code_schema
elif [ "$code_prime_1" != "0" ]; then
  echo "[test_multithreaded_proxy.sh]: Schema error"
  exit $code_prime_1
elif [ "$code_prime_2" != "0" ]; then
  echo "[test_multithreaded_proxy.sh]: Schema error"
  exit $code_prime_2
elif [ "$code1" != "0" ]; then
  echo "[test_multithreaded_proxy.sh]: Schema error"
  exit $code1
elif [ "$code2" != "0" ]; then
  echo "[test_multithreaded_proxy.sh]: Schema error"
  exit $code2
elif [ "$code_proxy" != "0" ]; then
  echo "[test_multithreaded_proxy.sh]: Proxy error"
  exit $code_proxy
fi
echo "[test_multithreaded_proxy.sh]: Passed!"
echo "..................................................."
echo "..................................................."
