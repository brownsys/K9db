if [[ -f /tmp/trace.sql ]]; then
  rm /tmp/trace.sql
fi
cat experiments/lobsters/schema-simplified.sql >> /tmp/trace.sql
cat experiments/lobsters/queries.sql >> /tmp/trace.sql

bin/drop.sh pelton pelton
bazel run //bin:cli -- --logtostderr < /tmp/trace.sql
