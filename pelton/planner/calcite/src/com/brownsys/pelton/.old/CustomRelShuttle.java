package com.brownsys.pelton.planner;

import com.brownsys.pelton.planner.graph.Edge;
import com.brownsys.pelton.planner.graph.Operator;
import com.brownsys.pelton.planner.operators.AggregateOperator;
import com.brownsys.pelton.planner.operators.FilterOperator;
import com.brownsys.pelton.planner.operators.InputOperator;
import com.brownsys.pelton.planner.operators.ProjectOperator;
import com.brownsys.pelton.planner.operators.UnionOperator;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomRelShuttle extends RelShuttleImpl {
  final Logger LOGGER = LoggerFactory.getLogger(CustomRelShuttle.class);
  private ArrayList<Operator> nodes = new ArrayList<Operator>();
  private ArrayList<Edge> edges = new ArrayList<Edge>();

  public ArrayList<Operator> getNodes() {
    return nodes;
  }

  public ArrayList<Edge> getEdges() {
    return edges;
  }

  public ArrayList<String> getSchema(RelDataType rt) {
    ArrayList<String> schema = new ArrayList<String>();
    for (RelDataTypeField field : rt.getFieldList()) {
      schema.add(field.getType().getSqlTypeName().toString());
    }
    return schema;
  }

  @Override
  public RelNode visit(LogicalFilter filter) {
    FilterOperator filterOp = new FilterOperator();
    ArrayList<Integer> cids = new ArrayList<Integer>();
    ArrayList<String> ops = new ArrayList<String>();
    ArrayList<String> vals = new ArrayList<String>();
    ArrayList<String> valTypes = new ArrayList<String>();

    filterOp.setId(filter.getId());

    // Comparison operators supported by Pelton's filter
    ArrayList<SqlKind> suppCompOps = new ArrayList<SqlKind>();
    suppCompOps.add(SqlKind.EQUALS);
    suppCompOps.add(SqlKind.LESS_THAN);
    suppCompOps.add(SqlKind.LESS_THAN_OR_EQUAL);
    suppCompOps.add(SqlKind.GREATER_THAN);
    suppCompOps.add(SqlKind.GREATER_THAN_OR_EQUAL);
    suppCompOps.add(SqlKind.NOT_EQUALS);

    RexCall opCall = (RexCall) filter.getCondition();
    ImmutableList<RexNode> operands = opCall.operands;
    if (opCall.isA(SqlKind.AND)) {
      for (RexNode item : operands) {
        RexCall subOpCall = (RexCall) item;
        if (!subOpCall.isA(suppCompOps)) {
          LOGGER.error("Unimplemented comparision: " + opCall.getKind().toString());
        }
        assert subOpCall.operands.get(0) instanceof RexInputRef;
        assert subOpCall.operands.get(1) instanceof RexLiteral;
        // Note: cast sql call occurs here if size is given for integer
        cids.add(((RexInputRef) subOpCall.operands.get(0)).getIndex());
        ops.add(subOpCall.getOperator().toString());
        vals.add(((RexLiteral) subOpCall.operands.get(1)).toString());
        valTypes.add(((RexLiteral) subOpCall.operands.get(1)).getTypeName().toString());
      }
    } else if (opCall.isA(SqlKind.OR)) {
      // TODO: Support OR condition in pelton's filter operator
      LOGGER.error("Support for OR condition yet to be implemented in pelton's filter operator.");

    } else if (opCall.isA(suppCompOps)) {
      // single comparison
      assert operands.get(0) instanceof RexInputRef;
      // TODO: Discuss support for subqueries in pelton
      // If this is a subquery then the RHS will not be a literal
      assert operands.get(1) instanceof RexLiteral;
      cids.add(((RexInputRef) operands.get(0)).getIndex());
      ops.add(opCall.getOperator().toString());
      vals.add(((RexLiteral) operands.get(1)).toString());
      valTypes.add(((RexLiteral) operands.get(1)).getTypeName().toString());

    } else {
      LOGGER.error("Unimplemented comparision: " + opCall.getKind().toString());
    }

    filterOp.setCids(cids);
    filterOp.setOps(ops);
    filterOp.setVals(vals);
    filterOp.setValTypes(valTypes);
    filterOp.setOutSchema(getSchema(filter.getRowType()));
    nodes.add(filterOp);
    edges.add(new Edge(filter.getInput().getId(), filterOp.getId()));
    return visitChild(filter, 0, filter.getInput());
  }

  @Override
  public RelNode visit(LogicalProject project) {
    ProjectOperator projectOp = new ProjectOperator();
    projectOp.setId(project.getId());
    ArrayList<Integer> cids = new ArrayList<Integer>();
    for (RelDataTypeField item : project.getRowType().getFieldList()) {
      cids.add(item.getIndex());
    }

    projectOp.setCids(cids);
    projectOp.setOutSchema(getSchema(project.getRowType()));
    nodes.add(projectOp);
    edges.add(new Edge(project.getInput().getId(), projectOp.getId()));
    return visitChild(project, 0, project.getInput());
  }

  @Override
  public RelNode visit(TableScan scan) {
    InputOperator inputOp = new InputOperator();
    inputOp.setId(scan.getId());
    // second element is the table name
    inputOp.setTableName(scan.getTable().getQualifiedName().get(1));
    inputOp.setOutSchema(getSchema(scan.getRowType()));
    nodes.add(inputOp);
    // don't add edge, does not have a "parent"
    return scan;
  }

  @Override
  public RelNode visit(LogicalUnion union) {
    UnionOperator unionOp = new UnionOperator();
    unionOp.setId(union.getId());
    for (RelNode input : union.getInputs()) {
      edges.add(new Edge(input.getId(), unionOp.getId()));
    }
    unionOp.setOutSchema(getSchema(union.getRowType()));
    nodes.add(unionOp);
    return visitChildren(union);
  }

  @Override
  public RelNode visit(LogicalAggregate aggregate) {
    AggregateOperator aggregateOp = new AggregateOperator();
    aggregateOp.setId(aggregate.getId());
    List<Integer> groupCols = aggregate.getGroupSet().toList();
    ArrayList<String> aggFuncs = new ArrayList<String>();
    ArrayList<Integer> aggCols = new ArrayList<Integer>();

    for (AggregateCall call : aggregate.getAggCallList()) {
      aggFuncs.add(call.getAggregation().toString());
      aggCols.add(call.getArgList().get(0));
    }

    aggregateOp.setGroupCols(groupCols);
    aggregateOp.setAggFuncs(aggFuncs);
    aggregateOp.setAggCols(aggCols);
    aggregateOp.setOutSchema(getSchema(aggregate.getRowType()));
    nodes.add(aggregateOp);
    edges.add(new Edge(aggregate.getInput().getId(), aggregateOp.getId()));
    return visitChild(aggregate, 0, aggregate.getInput());
  }
}
