package com.brownsys.pelton.planner;

import com.google.common.io.Resources;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.*;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class QueryPlanner {

  private final Planner planner;
  final static Logger LOGGER = LoggerFactory.getLogger(QueryPlanner.class);

  public QueryPlanner(SchemaPlus schema) {
    final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();

    traitDefs.add(ConventionTraitDef.INSTANCE);
    traitDefs.add(RelCollationTraitDef.INSTANCE);

    FrameworkConfig calciteFrameworkConfig = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.configBuilder()
            // Lexical configuration defines how identifiers are quoted, whether they are converted to upper or lower
            // case when they are read, and whether identifiers are matched case-sensitively.
            .setLex(Lex.MYSQL)
            .build())
        // Sets the schema to use by the planner
        .defaultSchema(schema)
        .traitDefs(traitDefs)
        // Context provides a way to store data within the planner session that can be accessed in planner rules.
        .context(Contexts.EMPTY_CONTEXT)
        // Rule sets to use in transformation phases. Each transformation phase can use a different set of rules.
        .ruleSets(RuleSets.ofList())
        // Custom cost factory to use during optimization
        .costFactory(null)
        .typeSystem(RelDataTypeSystem.DEFAULT)
        .build();

    this.planner = Frameworks.getPlanner(calciteFrameworkConfig);
  }

  public RelNode getLogicalPlan(String query) throws ValidationException, RelConversionException {
    SqlNode sqlNode;

    try {
      sqlNode = planner.parse(query);
    } catch (SqlParseException e) {
      throw new RuntimeException("Query parsing error.", e);
    }

    SqlNode validatedSqlNode = planner.validate(sqlNode);

    return planner.rel(validatedSqlNode).project();
  }
  
  public void traversePlan(RelNode logicalPlan) throws JsonGenerationException, JsonMappingException, IOException {
	  CustomRelShuttle shuttle = new CustomRelShuttle();
	  logicalPlan.accept(shuttle);
	  
	  ArrayList<Operator> nodes = shuttle.getNodes();
	  ArrayList<Edge> edges = shuttle.getEdges();
	  
	  // add a matview operator; first node is the "root" of tree having max id
	  MatviewOperator matviewOp = new MatviewOperator();
	  matviewOp.setId(nodes.get(0).getId()+1);
	  edges.add(0, new Edge(nodes.get(0).getId(), matviewOp.getId()));
	  nodes.add(0,matviewOp);
	  
	  // write as JSON
	  ObjectMapper mapper = new ObjectMapper();
	  ObjectWriter writer = mapper.writer(new DefaultPrettyPrinter());
	  writer.writeValue(Paths.get("nodes.json").toFile(), nodes);
	  writer.writeValue(Paths.get("edges.json").toFile(), edges);
	  LOGGER.info("Nodes written to nodes.json");
	  LOGGER.info("Edges written to edges.json");
  }

  public static void main(String[] args) throws IOException, SQLException, ValidationException, RelConversionException, NoSuchFieldException, SecurityException {
	
	if(args.length !=1) {
    	LOGGER.error("Enter the query as a single argument surrounded by quotes");
    }
	// Simple connection implementation for loading schema
    CalciteConnection connection = new SimpleCalciteConnection();
    String customSchema = Resources.toString(QueryPlanner.class.getResource("/generic.json"), Charset.defaultCharset());
    // ModelHandler reads the  schema and loads the schema to connection's root schema and sets the default schema
    new ModelHandler(connection, "inline:" + customSchema);

    QueryPlanner queryPlanner = new QueryPlanner(connection.getRootSchema().getSubSchema(connection.getSchema())); 
    LOGGER.info("Processing query: " + args[0]);
    RelNode logicalPlan = queryPlanner.getLogicalPlan(args[0]);
    LOGGER.info("Generated logical plan: \n" + RelOptUtil.toString(logicalPlan));
    queryPlanner.traversePlan(logicalPlan);
  }
}
