# Example

```bash
bazel run :benchmark -c opt -- --direct-shares-per-file 3 --group-shares-per-file 3 --in_size 100 --users-per-group 6 --write_every 0 --operations 300 --num-users 1000 --backend pelton --perf

bazel run :benchmark -c opt -- --direct-shares-per-file 3 --group-shares-per-file 3 --in_size 100 --users-per-group 6 --write_every 20 --operations 1000 --num-users 1000 --backend pelton
bazel run :benchmark -c opt -- --direct-shares-per-file 3 --group-shares-per-file 3 --in_size 100 --users-per-group 6 --write_every 20 --operations 1000 --num-users 1000 --backend mariadb
bazel run :benchmark -c opt -- --direct-shares-per-file 3 --group-shares-per-file 3 --in_size 100 --users-per-group 6 --write_every 20 --operations 1000 --num-users 1000 --backend memcached
```



--> Priming done in 59721ms
Read: 951
Read [50]: 6614
Read [90]: 7862
Read [95]: 9749
Read [99]: 14236
Direct: 25
Direct [50]: 4733
Direct [90]: 5051
Direct [95]: 6884
Direct [99]: 22905
Group: 24
Group [50]: 4630
Group [90]: 4861
Group [95]: 4861
Group [99]: 5259

