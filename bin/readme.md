# Correctness Testing Usage

## TLDR;

`bazel run //bin:correctness` runs ~35 lobster test queries!

## Run individual queries

`bazel run //bin:correctness -- --schema=<schema.sql> --queries=<query.sql> expected_output=<expected_result> inserts=<inserts.sql>`

- For example:

`bazel run //bin:correctness -- --schema=bin/data/lobster_schema_simplified.sql queries=bin/data/lobster_q1.sql expected_output=bin/data/q1.txt inserts=bin/data/lobster_inserts.sql`

## Add custom tests

1. Create four files containing:

- the schema,
- the inserts
- the queries (including deletes, updates, views, and selects),
- and the expected query results.

2. Add their filenames to `/bin/data/BUILD.bazel` and to `/bin/BUILD.bazel` under the `data = ` for `correctness.cc`

3. Run `correctness.cc`, passing the files containing the schema, queries, and expected results as command line arguments. Running `drop.sh` may be helpful for removing databases from previous runs.

- For example:  
   `./bin/drop.sh pelton pelton && bazel run //bin:correctness -- --schema=bin/data/medical_chat_schema.sql --bin/data/medical_chat_queries.sql --bin/data/medical_chat_expected.txt`
