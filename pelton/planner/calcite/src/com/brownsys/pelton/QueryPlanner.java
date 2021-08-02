package com.brownsys.pelton;

import com.brownsys.pelton.nativelib.DataFlowGraphLibrary;
import com.brownsys.pelton.schema.PeltonSchemaFactory;
import java.util.Collections;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

public class QueryPlanner {
  // JNI wrapper around our generator API (dataflow/generator.h).
  private final DataFlowGraphLibrary.DataFlowGraphGenerator generator;
  // Calcite components required for planning.
  private final CalciteSchema schema;
  private final SqlParser.Config parserConfig;
  private final SqlValidator validator;
  private final SqlToRelConverter converter;
  private final HepPlanner planner;

  // Construct a QueryPlanner given a C++ DataFlowGraphLibrary interface exposing
  // Pelton state.
  private QueryPlanner(DataFlowGraphLibrary.DataFlowGraphGenerator generator) {
    // Store the c++ interface.
    this.generator = generator;

    // Create a calcite-understandable representation of the schema of the logical
    // un-sharded database, as exposed from c++ via generator.
    PeltonSchemaFactory schemaFactory = new PeltonSchemaFactory(generator);
    this.schema = schemaFactory.createSchema();

    // Configure the calcite syntax parsing.
    this.parserConfig =
        SqlParser.config().withLex(Lex.MYSQL).withConformance(SqlConformanceEnum.MYSQL_5);

    // Configure the calcite semantic validator.
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(
            this.schema,
            Collections.singletonList(schema.getName()),
            typeFactory,
            CalciteConnectionConfig.DEFAULT);
    SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT.withIdentifierExpansion(true);
    this.validator =
        SqlValidatorUtil.newValidator(
            SqlStdOperatorTable.instance(), catalogReader, typeFactory, validatorConfig);

    // Configure the HepPlanner.
    HepProgram hepProgram =
        HepProgram.builder()
            .addRuleInstance(CoreRules.AGGREGATE_JOIN_TRANSPOSE)
            .addRuleInstance(CoreRules.FILTER_AGGREGATE_TRANSPOSE)
            .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
            .build();
    this.planner = new HepPlanner(hepProgram);

    // Configure calcite's AST to logical operators transformer.
    this.converter =
        new SqlToRelConverter(
            null,
            validator,
            catalogReader,
            RelOptCluster.create(this.planner, new RexBuilder(typeFactory)),
            StandardConvertletTable.INSTANCE,
            SqlToRelConverter.config());
  }

  // Parse query and plan it into a physical plan.
  private RelNode getPhysicalPlan(String query)
      throws SqlParseException, ValidationException, RelConversionException {
    // Parse query.
    SqlNode sqlNode = SqlParser.create(query, this.parserConfig).parseStmt();
    // Validate query.
    SqlNode validatedSqlNode = this.validator.validate(sqlNode);
    // Transform to a tree of calcite logical operators.
    RelRoot rawPlan = this.converter.convertQuery(validatedSqlNode, false, true);
    System.out.println("Raw plan:");
    System.out.println(RelOptUtil.toString(rawPlan.project()));
    // Apply planner optimizations.
    this.planner.clear();
    this.planner.setRoot(rawPlan.project());
    return this.planner.findBestExp();
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
      throws SqlParseException, ValidationException, RelConversionException {
    System.out.println("Java: planning query");
    System.out.println(query);
    // Create the generator interface using the pointers passed from c++.
    DataFlowGraphLibrary.DataFlowGraphGenerator generator =
        new DataFlowGraphLibrary.DataFlowGraphGenerator(ptr1, ptr2);

    // Create a planner.
    QueryPlanner planner = new QueryPlanner(generator);

    // Parse the query and turn it into an optimized physical plan.
    RelNode physicalPlan = planner.getPhysicalPlan(query);
    System.out.println("Java: Calcite plan");
    System.out.println(RelOptUtil.toString(physicalPlan));

    // Extract pelton operators from plan and populate them into the c++ DataFlowGraph.
    planner.PopulateDataFlowGraph(physicalPlan);
    System.out.println("Java: Added all operators");
    System.out.println(generator.DebugString().getString());
    System.out.println("");
  }
}
