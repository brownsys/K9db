package com.brownsys.pelton;

import com.brownsys.pelton.nativelib.DataFlowGraphLibrary;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Stack;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

public class PhysicalPlanVisitor extends RelShuttleImpl {
  private static final List<SqlKind> FILTER_OPERATIONS =
      Arrays.asList(
          SqlKind.EQUALS,
          SqlKind.NOT_EQUALS,
          SqlKind.LESS_THAN,
          SqlKind.LESS_THAN_OR_EQUAL,
          SqlKind.GREATER_THAN,
          SqlKind.GREATER_THAN_OR_EQUAL);

  private final DataFlowGraphLibrary.DataFlowGraphGenerator generator;
  private final Stack<ArrayList<Integer>> childrenOperators;
  private final Hashtable<String, Integer> tableToInputOperator;

  public PhysicalPlanVisitor(DataFlowGraphLibrary.DataFlowGraphGenerator generator) {
    this.generator = generator;
    this.childrenOperators = new Stack<ArrayList<Integer>>();
    this.tableToInputOperator = new Hashtable<String, Integer>();
  }

  public void populateGraph(RelNode plan) {
    this.childrenOperators.push(new ArrayList<Integer>());

    // Start visting.
    plan.accept(this);

    // Add a materialized view linked to the last node in the graph.
    ArrayList<Integer> children = this.childrenOperators.pop();
    assert children.size() == 1;

    // TODO(babman): deduce the key columns from schema?
    this.generator.AddMatviewOperator(children.get(0), new int[] {0});
    assert this.childrenOperators.isEmpty();
  }

  @Override
  public RelNode visit(TableScan scan) {
    System.out.println("input");
    String tableName = scan.getTable().getQualifiedName().get(0);
    Integer inputOperator = this.tableToInputOperator.get(tableName);
    if (inputOperator == null) {
      inputOperator = this.generator.AddInputOperator(tableName);
      this.tableToInputOperator.put(tableName, inputOperator);
    }
    this.childrenOperators.peek().add(inputOperator);
    return scan;
  }

  @Override
  public RelNode visit(LogicalUnion union) {
    System.out.println("union");
    // Add a new level in the stack to store ids of the direct children operators.
    this.childrenOperators.push(new ArrayList<Integer>());

    // Visit children.
    visitChildren(union);

    // Get IDs of generated children.
    ArrayList<Integer> children = this.childrenOperators.pop();
    int[] ids = new int[children.size()];
    for (int i = 0; i < children.size(); i++) {
      ids[i] = children.get(i);
    }

    // Add a union operator.
    int unionOperator = this.generator.AddUnionOperator(ids);
    this.childrenOperators.peek().add(unionOperator);

    return union;
  }

  private void visitFilterOperands(int filterOperator, RexNode condition, List<RexNode> operands) {
    // Must be a binary condition with one side being a column and the other being a literal.
    assert operands.size() == 2;
    assert operands.get(0) instanceof RexInputRef || operands.get(1) instanceof RexInputRef;
    assert operands.get(0) instanceof RexLiteral || operands.get(1) instanceof RexLiteral;

    // Get the input and the value expressions.
    int inputIndex = operands.get(0) instanceof RexInputRef ? 0 : 1;
    int valueIndex = (inputIndex + 1) % 2;
    RexInputRef input = (RexInputRef) operands.get(inputIndex);
    RexLiteral value = (RexLiteral) operands.get(valueIndex);

    // Determine the condition operation.
    int operationEnum = -1;
    switch (condition.getKind()) {
      case EQUALS:
        operationEnum = DataFlowGraphLibrary.EQUAL;
        break;
      case NOT_EQUALS:
        operationEnum = DataFlowGraphLibrary.NOT_EQUAL;
        break;
      case LESS_THAN:
        operationEnum = DataFlowGraphLibrary.LESS_THAN;
        break;
      case LESS_THAN_OR_EQUAL:
        operationEnum = DataFlowGraphLibrary.LESS_THAN_OR_EQUAL;
        break;
      case GREATER_THAN:
        operationEnum = DataFlowGraphLibrary.GREATER_THAN;
        break;
      case GREATER_THAN_OR_EQUAL:
        operationEnum = DataFlowGraphLibrary.GREATER_THAN_OR_EQUAL;
        break;
      default:
        assert false;
    }

    // Determine the value type.
    int columnId = input.getIndex();
    switch (value.getTypeName()) {
      case DECIMAL:
      case INTEGER:
        this.generator.AddFilterOperationSigned(
            filterOperator, RexLiteral.intValue(value), columnId, operationEnum);
        break;
      case VARCHAR:
        this.generator.AddFilterOperation(
            filterOperator, RexLiteral.stringValue(value), columnId, operationEnum);
        break;
      default:
        throw new IllegalArgumentException("Invalid literal type in filter");
    }
  }

  @Override
  public RelNode visit(LogicalFilter filter) {
    System.out.println("filter");
    RexNode condition = filter.getCondition();
    assert condition instanceof RexCall;
    assert condition.isA(SqlKind.AND) || condition.isA(FILTER_OPERATIONS);

    // Add a new level in the stack to store ids of the direct children operators.
    this.childrenOperators.push(new ArrayList<Integer>());

    // Visit children.
    visitChildren(filter);

    // Get IDs of generated children.
    ArrayList<Integer> children = this.childrenOperators.pop();
    assert children.size() == 1;

    // Add a filter operator.
    int filterOperator = this.generator.AddFilterOperator(children.get(0));
    this.childrenOperators.peek().add(filterOperator);

    // Fill the filter operator with the appropriate condition(s).
    if (condition.isA(FILTER_OPERATIONS)) {
      List<RexNode> operands = ((RexCall) condition).getOperands();
      this.visitFilterOperands(filterOperator, condition, operands);
    }

    if (condition.isA(SqlKind.AND)) {
      for (RexNode operand : ((RexCall) condition).getOperands()) {
        List<RexNode> operands = ((RexCall) operand).getOperands();
        this.visitFilterOperands(filterOperator, operand, operands);
      }
    }
    return filter;
  }

  /*
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
  */
}
