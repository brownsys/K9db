# Example

```bash
bazel run :benchmark -c opt -- \
  --num-users 5000 \
  --users-per-group 5 \
  --files-per-user 3 \
  --direct-shares-per-file 3 \
  --group-shares-per-file 2 \
  --write_every 20 \
  --in_size 50 \
  --operations 500 \
  --backend pelton
  # --warmup 500
  # ^ for memcached.
```
