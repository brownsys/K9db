# Correctness Testing Usage

## TLDR;

`bazel run //bin:correctness` runs a bunch of tests!

## Run individual queries

`bazel run //bin:correctness -- <schema.sql> <query.sql> <expected_result> <inserts.sql>`

- For example:

`bazel run //bin:correctness -- bin/data/lobster_schema_simplified.sql bin/data/lobster_q1.sql bin/data/q1.txt bin/data/lobster_inserts.sql`

## Add custom tests

1. Create four files containing:

- the schema,
- the inserts
- the queries (including deletes, updates, views, and selects),
- and the expected query results.

2. Add their filenames to `/bin/data/BUILD.bazel` and to `/bin/BUILD.bazel` under the `data = ` for `correctness.cc`

3. Run `correctness.cc`, passing the files containing the schema, queries, and expected results as command line arguments. Running `drop.sh` may be helpful for removing databases from previous runs.

- For example:  
   `./bin/drop.sh pelton pelton && bazel run //bin:correctness -- bin/data/medical_chat_schema.sql bin/data/medical_chat_queries.sql bin/data/medical_chat_expected.txt`
