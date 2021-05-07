#!/bin/bash
if [[ -f trace.sql ]]; then
  rm trace.sql
fi

# Schema.
echo "SET echo;" > trace.sql

cat experiments/lobsters/schema-simplified.sql >> trace.sql
echo "" >> trace.sql
echo "" >> trace.sql

# Views.
cat experiments/lobsters/views.sql >> trace.sql
echo "" >> trace.sql
echo "" >> trace.sql

# Load inserts.
cat experiments/lobsters/traces/mini-populate.sql >> trace.sql
echo "" >> trace.sql
echo "" >> trace.sql

# Put in some endpoint.
echo "# perf start" >> trace.sql
cat experiments/lobsters/traces/minis/$1.sql >> trace.sql

