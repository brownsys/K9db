# Correctness Testing Usage

## Run existing tests

1. Run `correctness.cc`, passing the files containing the schema, queries, and expected results as command line arguments

- For example:  
   `bazel build ... && ./bin/drop.sh pelton pelton && bazel run //bin:correctness -- bin/data/medical_chat_schema.sql bin/data/medical_chat_queries.sql bin/data/medical_chat_expected.txt`

  `bazel build ... && ./bin/drop.sh pelton pelton && bazel run //bin:correctness -- bin/data/lobster_schema.sql bin/data/lobster_queries.sql bin/data/lobster_expected.txt`

## Add custom tests

1. Create three files containing:

- the schema,
- the queries (including inserts, deletes, updates, views, and selects),
- and the expected query results.

2. Add their filenames to `/bin/data/BUILD.bazel` and to `/bin/BUILD.bazel` under the `data = ...` for `correctness.cc`
