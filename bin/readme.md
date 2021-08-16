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

# Dataflow benchmark
The dataflow benchmark specifically benchmarks the compute performance of the sharded dataflow. It first constructs all the records in memory and then distributes them to the available workers. The workers then simultaneously dispatch record batches to the dataflow engine.

The benchmark suite logs the time (from the recent epoch) in milliseconds before starting the workers. The benchmark sets appropriate markers in the partitioned matviews. Currently, if the number of records processed by the matview equals it's `marker_` value, then it logs the time in milliseconds. The time taken for the sharded dataflow to process all records would be the difference of the largest logged time by a partitioned matview and the start time logged by the benchmarking suite.

- Ensure that #batches % #workers = 0, so that the records can be evenly distributed among the workers.
- Graph types supported by the benchmark can be found in `pelton/dataflow/benchmark/utils.h`.

To run the benchmark, execute:
```
bazel run bin:benchmark --config=opt --  --graph_type=0 --num_batches=300000 --batch_size=1 --num_workers=3 --num_partitions=3
```
