# Query Planner

The query planner makes use of Apache Calcite for query planning. The planner generates the logical plan based based on the provided SQL query. The query is first parsed and then validated in order to generate the logical plan. Calcite requires info about the table schema in order to validate the AST. Once the logical plan is generated, the graph is exported in JSON along with necessary data that would be required by pelton to generate the corresponding dataflow graph.

## Building

```
mvn clean compile assembly:single
```

## Running and examples

There are three steps to generate and export the plan:
1. Specify the schema in `src/main/resources/table-schemas.json`. (specify `colSize` as -1 for columns that have no max size specified)
2. Update `src/main/resources/generic.json` to reflect the above schema. This is what directs calcite to make use of a custom table factory for a particular schema.
3. Execute the jar in `target/` with the SQL query as the CLI argument.

The planner currently supports very basic queries which can be mapped to pelton. Following are some examples of supported queries specific to schema in `table-schemas.json`:

```
java -jar target/<jar-file> "select * from orders where product='xzy'"
```

```
java -jar target/<jar-file> "select * from orders where units>4 and id>3"
```

```
java -jar target/<jar-file> "select product,sum(units) from orders group by product"
```

```
java -jar target/<jar-file> "(select id from orders) union (select id from products)"
```
