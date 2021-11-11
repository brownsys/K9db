# #!/bin/bash
./bin/drop.sh root password

# # test lobsters (dataflow)
bazel run --run_under "valgrind --error-exitcode=1 --errors-for-leak-kinds=definite --leak-check=full --show-leak-kinds=definite" //mysql_proxy/src:mysql_proxy -- -logtostderr=1 &
sleep 20
proxy_pid=$!
mariadb --port=10001 --host=127.0.0.1 < bin/sync_testing/proxy_tests/lobsters_schema_simplified.sql &&
mariadb --port=10001 --host=127.0.0.1 < bin/sync_testing/proxy_tests/lobsters_views.sql &&
sleep 30
mariadb --port=10001 --host=127.0.0.1 < bin/sync_testing/proxy_tests/lobsters_inserts1.sql &
mariadb --port=10001 --host=127.0.0.1 < bin/sync_testing/proxy_tests/lobsters_inserts2.sql 
mariadb --port=10001 --host=127.0.0.1 < bin/sync_testing/proxy_tests/lobsters_queries1.sql &
mariadb --port=10001 --host=127.0.0.1 < bin/sync_testing/proxy_tests/lobsters_queries2.sql
sleep 60
client_pid=$!
kill $client_pid
kill $proxy_pid
./bin/drop.sh root password

# # test lobsters and social chat (dataflow)
bazel run --run_under "valgrind --error-exitcode=1 --errors-for-leak-kinds=definite --leak-check=full --show-leak-kinds=definite" //mysql_proxy/src:mysql_proxy -- -logtostderr=1 &
sleep 20
proxy_pid=$!
mariadb --port=10001 --host=127.0.0.1 < bin/sync_testing/proxy_tests/lobsters_schema_simplified.sql &&
mariadb --port=10001 --host=127.0.0.1 < bin/sync_testing/proxy_tests/social_chat_schema.sql &&
mariadb --port=10001 --host=127.0.0.1 < bin/sync_testing/proxy_tests/lobsters_views.sql &&
sleep 30
mariadb --port=10001 --host=127.0.0.1 < bin/sync_testing/proxy_tests/lobsters_inserts.sql &
mariadb --port=10001 --host=127.0.0.1 < bin/sync_testing/proxy_tests/lobsters_queries.sql
sleep 60
client_pid=$!
kill $client_pid
kill $proxy_pid
./bin/drop.sh root password

# test concurrent creates
bazel run --run_under "valgrind --error-exitcode=1 --errors-for-leak-kinds=definite --leak-check=full --show-leak-kinds=definite" //mysql_proxy/src:mysql_proxy -- -logtostderr=1 &
sleep 20
proxy_pid=$!
mariadb --port=10001 --host=127.0.0.1 < bin/sync_testing/sharder_tests/correctness/creates_simple1.sql &
mariadb --port=10001 --host=127.0.0.1 < bin/sync_testing/sharder_tests/correctness/creates_simple2.sql
sleep 60
client_pid=$!
kill $client_pid
kill $proxy_pid
./bin/drop.sh root password

# # test concurrent inserts
bazel run --run_under "valgrind --error-exitcode=1 --errors-for-leak-kinds=definite --leak-check=full --show-leak-kinds=definite" //mysql_proxy/src:mysql_proxy -- -logtostderr=1 &
sleep 20
proxy_pid=$!
mariadb --port=10001 --host=127.0.0.1 < bin/sync_testing/sharder_tests/correctness/creates_unique1.sql &
mariadb --port=10001 --host=127.0.0.1 < bin/sync_testing/sharder_tests/correctness/creates_unique2.sql
sleep 30
mariadb --port=10001 --host=127.0.0.1 < bin/sync_testing/sharder_tests/correctness/inserts_unique1.sql &
mariadb --port=10001 --host=127.0.0.1 < bin/sync_testing/sharder_tests/correctness/inserts_unique2.sql
sleep 60
client_pid=$!
kill $client_pid
kill $proxy_pid
./bin/drop.sh root password