bazel run :vote-benchmark -c opt -- \
    --articles 100                  \
    --distribution uniform          \
    --target 100                    \
    --write-every 19                \
    --runtime 30                    \
    pelton


(or memcached-hybrid or mysql)
