#!/bin/bash
./bin/drop.sh root password

# test simple-websubmit
bazel run --run_under "valgrind --error-exitcode=1 --errors-for-leak-kinds=definite --leak-check=full --show-leak-kinds=definite" //mysql_proxy/src:mysql_proxy -- -logtostderr=1 &
pid=$!
sleep 20
mariadb --port=10001 --host=127.0.0.1 < bin/examples/simple-websubmit.sql
kill $pid
./bin/drop.sh root password

# test medical_chat 
bazel run --run_under "valgrind --error-exitcode=1 --errors-for-leak-kinds=definite --leak-check=full --show-leak-kinds=definite" //mysql_proxy/src:mysql_proxy -- -logtostderr=1 &
pid=$!
sleep 20
mariadb --port=10001 --host=127.0.0.1 < bin/examples/medical_chat.sql
kill $pid
./bin/drop.sh root password

# test social_chat
bazel run --run_under "valgrind --error-exitcode=1 --errors-for-leak-kinds=definite --leak-check=full --show-leak-kinds=definite" //mysql_proxy/src:mysql_proxy -- -logtostderr=1 &
pid=$!
sleep 20
mariadb --port=10001 --host=127.0.0.1 < bin/examples/social_chat.sql
kill $pid
./bin/drop.sh root password
