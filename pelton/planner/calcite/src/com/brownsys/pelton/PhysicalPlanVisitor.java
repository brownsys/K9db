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
import org.apache.calcite.rel.logical.LogicalJoin;
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

    // The key of this matview is automatically determine by the input schema to it.
    this.generator.AddMatviewOperator(children.get(0));
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
      case CHAR:
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

  @Override
  public RelNode visit(LogicalJoin join) {
    System.out.println("join");
    // Add a new level in the stack to store ids of the direct children operators.
    this.childrenOperators.push(new ArrayList<Integer>());

    // Visit children.
    visitChildren(join);

    // Find the size of the schema of the left relation.
    int leftCount = join.getLeft().getRowType().getFieldCount();

    // Visit the join condition, extract the column ids for the left and right relation.
    RexNode condition = join.getCondition();

    // Only support == for now.
    assert condition.isA(SqlKind.EQUALS);
    List<RexNode> operands = ((RexCall) condition).getOperands();
    assert operands.size() == 2;

    // Join condition must be over a column per input relation.
    RexNode leftCondition = operands.get(0);
    RexNode rightCondition = operands.get(1);
    assert leftCondition instanceof RexInputRef;
    assert rightCondition instanceof RexInputRef;
    int leftColIndex = ((RexInputRef) leftCondition).getIndex();
    int rightColIndex = ((RexInputRef) rightCondition).getIndex();
    rightColIndex -= leftCount;

    // Set up all join operator parameters.
    ArrayList<Integer> children = this.childrenOperators.pop();
    int leftInput = children.get(0);
    int rightInput = children.get(1);

    // Find the right join operator per join type.
    int joinOperator = -1;
    System.out.println(join.getJoinType());
    switch (join.getJoinType()) {
      case INNER:
        joinOperator =
            this.generator.AddEquiJoinOperator(leftInput, rightInput, leftColIndex, rightColIndex);
        break;
      case ANTI:
      case FULL:
      case LEFT:
      case RIGHT:
      case SEMI:
        throw new IllegalArgumentException("Unsupported join type " + join.getJoinType());
    }

    // Propogate up to parents.
    this.childrenOperators.peek().add(joinOperator);
    return join;
  }
}
