# Running the harness

## Baseline
```bash
bazel run -c opt //:lobsters-harness -- \
  --runtime 60 \
  --datascale 0.05 \
  --reqscale 10 \
  --queries pelton \
  --backend rocks-mariadb \
  --prime \
  "mysql://pelton:password@localhost:3306/lobsters"
```

## Pelton
```bash
bazel run -c opt //:lobsters-harness -- \
  --runtime 60 \
  --datascale 0.05 \
  --reqscale 10 \
  --queries pelton \
  --backend pelton \
  --prime \
  --in-flight 1 \
  "mysql://pelton:password@localhost:10001/lobsters"
```
