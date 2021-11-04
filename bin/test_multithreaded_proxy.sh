#!/bin/bash
./bin/drop.sh root password

# test lobsters
bazel run --run_under "valgrind --error-exitcode=1 --errors-for-leak-kinds=definite --leak-check=full --show-leak-kinds=definite" //mysql_proxy/src:mysql_proxy -- -logtostderr=1 &
# bazel run //mysql_proxy/src:mysql_proxy -- -logtostderr=1 &
sleep 20
mariadb --host=127.0.0.1 --port=10001 < bin/sync_testing/proxy_tests/lobsters_schema_simplified.sql &&
mariadb --host=127.0.0.1 --port=10001 < bin/sync_testing/proxy_tests/lobsters_views.sql &&
sleep 20
mariadb --host=127.0.0.1 --port=10001 < bin/sync_testing/proxy_tests/lobsters_inserts1.sql &
mariadb --host=127.0.0.1 --port=10001 < bin/sync_testing/proxy_tests/lobsters_inserts2.sql 
mariadb --host=127.0.0.1 --port=10001 < bin/sync_testing/proxy_tests/lobsters_queries1.sql &
mariadb --host=127.0.0.1 --port=10001 < bin/sync_testing/proxy_tests/lobsters_queries2.sql

sleep 60
pid=$!
kill $pid
./bin/drop.sh root password

# test lobsters and social chat
bazel run --run_under "valgrind --error-exitcode=1 --errors-for-leak-kinds=definite --leak-check=full --show-leak-kinds=definite" //mysql_proxy/src:mysql_proxy -- -logtostderr=1 &
# bazel run //mysql_proxy/src:mysql_proxy -- -logtostderr=1 &
sleep 20
mariadb --host=127.0.0.1 --port=10001 < bin/sync_testing/proxy_tests/lobsters_schema_simplified.sql &&
mariadb --host=127.0.0.1 --port=10001 < bin/sync_testing/proxy_tests/social_chat_schema.sql &&
mariadb --host=127.0.0.1 --port=10001 < bin/sync_testing/proxy_tests/lobsters_views.sql &&
sleep 20
mariadb --host=127.0.0.1 --port=10001 < bin/sync_testing/proxy_tests/lobsters_inserts.sql &
mariadb --host=127.0.0.1 --port=10001 < bin/sync_testing/proxy_tests/lobsters_queries.sql
sleep 60
pid=$!
kill $pid
./bin/drop.sh root password

# test concurrent creates
bazel run --run_under "valgrind --error-exitcode=1 --errors-for-leak-kinds=definite --leak-check=full --show-leak-kinds=definite" //mysql_proxy/src:mysql_proxy -- -logtostderr=1 &
# bazel run //mysql_proxy/src:mysql_proxy -- -logtostderr=1 &
sleep 20
mariadb --host=127.0.0.1 --port=10001 < bin/sync_testing/sharder_tests/correctness/creates_simple1.sql &
mariadb --host=127.0.0.1 --port=10001 < bin/sync_testing/sharder_tests/correctness/creates_simple2.sql
sleep 60
pid=$!
kill $pid
./bin/drop.sh root password

# test concurrent inserts
bazel run --run_under "valgrind --error-exitcode=1 --errors-for-leak-kinds=definite --leak-check=full --show-leak-kinds=definite" //mysql_proxy/src:mysql_proxy -- -logtostderr=1 &
# bazel run //mysql_proxy/src:mysql_proxy -- -logtostderr=1 &
sleep 20
mariadb --host=127.0.0.1 --port=10001 < bin/sync_testing/sharder_tests/correctness/creates_unique1.sql &
mariadb --host=127.0.0.1 --port=10001 < bin/sync_testing/sharder_tests/correctness/creates_unique2.sql
sleep 20
mariadb --host=127.0.0.1 --port=10001 < bin/sync_testing/sharder_tests/correctness/inserts_unique1.sql &
mariadb --host=127.0.0.1 --port=10001 < bin/sync_testing/sharder_tests/correctness/inserts_unique2.sql
sleep 60
pid=$!
kill $pid
./bin/drop.sh root password