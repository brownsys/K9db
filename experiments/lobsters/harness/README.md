Command to run the harness:
```
bazel run //experiments/lobsters/harness:lobsters-harness --config nightly -- --runtime 120 --datascale 0.1 --reqscale 1 --queries pelton --backend rocks-mariadb --prime "mysql://root:password@localhost:3306/lobsters"
```
