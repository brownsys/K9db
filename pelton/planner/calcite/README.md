# Query Planner

The query planner makes use of Apache Calcite for query planning.
The planner generates the logical plan based based on the provided SQL query.
The query is first parsed and then validated in order to generate the logical
plan. Calcite requires info about the table schema in order to validate the AST.
Once the logical plan is generated, the graph is exported in JSON along with
necessary data that would be required by pelton to generate the corresponding
dataflow graph.

## Building

The planner is built with bazel.

```bash
# from pelton root directory
bazel build //pelton/planner/...
```

## Running and examples

There are three steps to generate and export the plan:
1. Specify the schema in `src/main/resources/table-schemas.json`.
   Specify `colSize` as -1 for columns that have no max size.
2. Update `src/main/resources/generic.json` to reflect the above schema.
   This is what directs calcite to make use of a custom table factory for a
   particular schema.
3. Build the java files as above.
4. Run the planner and provide the query as a command line argument:
```bash
bazel run //pelton/planner/calcite:planner -- "query"
```

The planner currently supports very basic queries which can be mapped to pelton.
Following are some examples of supported queries specific to schema in
`table-schemas.json`:
```bash
bazel run //pelton/planner/calcite:planner -- "select * from orders where product='xzy'"
bazel run //pelton/planner/calcite:planner -- "select * from orders where units>4 and id>3"
bazel run //pelton/planner/calcite:planner -- "select product,sum(units) from orders group by product"
bazel run //pelton/planner/calcite:planner -- "(select id from orders) union (select id from products)"
```
