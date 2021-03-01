package com.brownsys.pelton;

import com.brownsys.pelton.nativelib.DataFlowGraphLibrary;
import com.brownsys.pelton.schema.PeltonSchemaFactory;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

public class QueryPlanner {
  private final DataFlowGraphLibrary.DataFlowGraphGenerator generator;
  private final SchemaPlus schema;
  private final Planner planner;

  // Construct a QueryPlanner given a C++ DataFlowGraphLibrary interface exposing
  // Pelton state.
  private QueryPlanner(DataFlowGraphLibrary.DataFlowGraphGenerator generator) {
    // Store the c++ interface.
    this.generator = generator;

    // Create a calcite-understandable representation of the schema of the logical
    // un-sharded database, as exposed from c++ via generator.
    PeltonSchemaFactory schemaFactory = new PeltonSchemaFactory(generator);
    this.schema = schemaFactory.createSchema();

    FrameworkConfig calciteFrameworkConfig =
        Frameworks.newConfigBuilder()
            // MySQL dialect.
            .parserConfig(SqlParser.configBuilder().setLex(Lex.MYSQL).build())
            // Sets the schema to use by the planner
            .defaultSchema(this.schema)
            // Context provides a way to store data within the planner session that can be
            // accessed in planner rules.
            .context(Contexts.EMPTY_CONTEXT)
            .build();

    this.planner = Frameworks.getPlanner(calciteFrameworkConfig);
  }

  // Parse query and plan it into a physical plan.
  private RelNode getPhysicalPlan(String query) throws ValidationException, RelConversionException {
    SqlNode sqlNode;

    try {
      sqlNode = this.planner.parse(query);
    } catch (SqlParseException e) {
      throw new RuntimeException("Query parsing error.", e);
    }

    SqlNode validatedSqlNode = this.planner.validate(sqlNode);
    return this.planner.rel(validatedSqlNode).project();
  }

  // Traverse the logical plan and use it to populate the corresponding operators into the graph in
  // this.generator.
  private void PopulateDataFlowGraph(RelNode physicalPlan) {
    PhysicalPlanVisitor visitor = new PhysicalPlanVisitor(this.generator);
    visitor.populateGraph(physicalPlan);
  }

  // This is the entry function that is invoked from pelton c++.
  // It is given two C-pointers: the first points to the output dataflow::DataFlowGraph that will be
  // populated
  // with the output graph. The second points to the current dataflow::DataFlowState of the system.
  // These pointers are passed to the native DataFlowGraphLibrary interface which interfaces between
  // C++ implementations
  // for reading and populating elements in these two pointers, and this java code.
  public static void plan(long ptr1, long ptr2, String query)
      throws ValidationException, RelConversionException {
    System.out.println("Java: planning query");
    System.out.println(query);
    // Create the generator interface using the pointers passed from c++.
    DataFlowGraphLibrary.DataFlowGraphGenerator generator =
        new DataFlowGraphLibrary.DataFlowGraphGenerator(ptr1, ptr2);

    // Create a planner.
    QueryPlanner planner = new QueryPlanner(generator);

    // Parse the query and turn it into an optimized physical plan.
    RelNode physicalPlan = planner.getPhysicalPlan(query);

    // Extract pelton operators from plan and populate them into the c++ DataFlowGraph.
    planner.PopulateDataFlowGraph(physicalPlan);
    System.out.println("Java: Added all operators");
  }
}
