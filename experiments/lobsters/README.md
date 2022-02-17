# Running the harness

## Baseline
```bash
bazel run //:lobsters-harness -- \
  --runtime 30 \
  --datascale 0.05 \
  --reqscale 1 \
  --queries pelton \
  --backend rocks-mariadb \
  --prime \
  "mysql://root:password@localhost:3306/lobsters"
```

## Pelton
```bash
bazel run //:lobsters-harness -- \
  --runtime 60 \
  --datascale 0.01 \
  --reqscale 10 \
  --queries pelton \
  --backend pelton \
  --prime \
  --in-flight 1 \
  "mysql://root:password@localhost:10001/lobsters"
```
