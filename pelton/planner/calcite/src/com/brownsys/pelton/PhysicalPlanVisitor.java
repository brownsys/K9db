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
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
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
  // Each filter operator in pelton performs either an OR or an AND operation
  // on it's operands. Nested conditions are represented as separae filter
  // operators and chained together after construction. The following map is
  // used to maintain a list of filter operators(either AND or OR) at a
  // particular LEVEL.
  // private final Hashtable<Integer, ArrayList<Integer>> filterOperators;

  public PhysicalPlanVisitor(DataFlowGraphLibrary.DataFlowGraphGenerator generator) {
    this.generator = generator;
    this.childrenOperators = new Stack<ArrayList<Integer>>();
    this.tableToInputOperator = new Hashtable<String, Integer>();
    // this.filterOperators = new Hashtable<Integer, ArrayList<Integer>>();
  }

  public void populateGraph(RelNode plan) {
    this.childrenOperators.push(new ArrayList<Integer>());

    // Start visting.
    plan.accept(this);

    // Check if a mat view operator was already created, otherwise create one.
    ArrayList<Integer> children = this.childrenOperators.pop();
    if (children.size() > 0) {
      // Add a materialized view linked to the last node in the graph.
      assert children.size() == 1;

      // The key of this matview is automatically determine by the input schema to it.
      int[] keyCols = new int[0]; // TODO(babman): figure out key from query.
      this.generator.AddMatviewOperator(children.get(0), keyCols);
      assert this.childrenOperators.isEmpty();
    }
  }

  // Created for queries that have an explicit order by, as well as unordered queries
  // that have a limit.
  @Override
  public RelNode visit(LogicalSort sort) {
    this.childrenOperators.push(new ArrayList<Integer>());

    // Start visting.
    visitChildren(sort);

    // Add a materialized view linked to the last node in the graph.
    ArrayList<Integer> children = this.childrenOperators.pop();
    assert children.size() == 1;

    // Find the sorting parameters.
    List<RexNode> exps = sort.getChildExps();
    int[] keyCols = new int[0]; // TODO(babman): figure out key from query.
    int[] sortingCols = new int[exps.size()];
    int limit = -1;
    int offset = 0;
    for (int i = 0; i < exps.size(); i++) {
      RexNode exp = exps.get(i);
      assert exp instanceof RexInputRef;
      sortingCols[i] = ((RexInputRef) exp).getIndex();
    }

    // If offset or fetch (e.g. limit) is null, it means the query does not contain a LIMIT (, )
    // expression. If they are not null, they may also be "?", in which case we keep the default
    // unspecified -1 values for them.
    if (sort.offset != null && !(sort.offset instanceof RexDynamicParam)) {
      assert sort.offset instanceof RexLiteral;
      offset = RexLiteral.intValue(sort.offset);
    }
    if (sort.fetch != null && !(sort.fetch instanceof RexDynamicParam)) {
      assert sort.fetch instanceof RexLiteral;
      limit = RexLiteral.intValue(sort.fetch);
    }

    // We do not add this to children, a matview cannot be a parent for other operators.
    this.generator.AddMatviewOperator(children.get(0), keyCols, sortingCols, limit, offset);
    return sort;
  }

  @Override
  public RelNode visit(TableScan scan) {
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

  private void addFilterOperation(int filterOperator, RexNode condition, List<RexNode> operands){
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

  private Integer visitFilterOperands(RexNode condition, List<RexNode> operands, Integer deepestFilterParent) {
      if(condition.isA(SqlKind.AND)){
        // If there are >1 nested conditions then connect them via a new union
        // operator else link the single child operator directly.
        // NOTE(Ishan): There is room for optimization here regarding chaining of
        // operators in certain scenarios instead of using a union operator,
        // but for lobesters' queries it won't make a difference.

        // Operands inserted in the following list are not nested
        List<RexNode> operations = new ArrayList<RexNode>();
        List<Integer> nestedOperators = new ArrayList<Integer>();
        for(RexNode operand : operands){
          if(operand.isA(SqlKind.OR)){
            nestedOperators.add(visitFilterOperands(operand, ((RexCall) operand).getOperands(), deepestFilterParent));
          } else if(operand.isA(SqlKind.AND)){
            nestedOperators.add(visitFilterOperands(operand, ((RexCall) operand).getOperands(), deepestFilterParent));
          } else{
            operations.add(operand);
          }
        }
        // We currently do not support 0 "base" operations, for example AND(OR, OR)
        assert operations.size()!=0;
        // Decide on parent(w.r.t the dataflow) of this filter operator
        int filterOperator;
        if(nestedOperators.size()==0){
          // Deepest filter operator with no further nested conditions
          filterOperator = this.generator.AddFilterOperator(deepestFilterParent);
        } else if(nestedOperators.size()==1){
          // Link the nested operator directly.
          filterOperator = this.generator.AddFilterOperator(nestedOperators.get(0));
        } else {
          // Link the nested operators via a union and link the current operator
          // with that union.
          int[] primitiveArray = nestedOperators.stream().mapToInt(i -> i).toArray();
          int unionOperator = this.generator.AddUnionOperator(primitiveArray);
          filterOperator = this.generator.AddFilterOperator(unionOperator);
        }
        // Add operations to the newly generated operator
        for(RexNode operation: operations){
          addFilterOperation(filterOperator, operation, ((RexCall) operation).getOperands());
        }
        return filterOperator;
      } else if (condition.isA(SqlKind.OR)){
        // We are following a pure union based  approach. Treat all the
        // operands as children of the union operator.
        // It does not matter whether the operand is nested or not.
        List<Integer> unionParents = new ArrayList<Integer>();
        for(RexNode operand: operands){
          unionParents.add(visitFilterOperands(operand, ((RexCall) operand).getOperands(), deepestFilterParent));
        }
        // Add union operator
        int[] primitiveArray = unionParents.stream().mapToInt(i -> i).toArray();
        int unionOperator = this.generator.AddUnionOperator(primitiveArray);
        return unionOperator;
      } else if (condition.isA(FILTER_OPERATIONS)){
        // Will only reach here if
        // 1. Either the core filter operator does not contain any nested conditions,
        // 2. or @param condition is part of the OR
        // condition. In this scenario, the constructed filter operator (a default AND
        // filter operator) will be the leaf. Hence it's parent will be
        // @deepestFilterParent
        int filterOperator = this.generator.AddFilterOperator(deepestFilterParent);
        addFilterOperation(filterOperator, condition, ((RexCall) condition).getOperands());
        return filterOperator;
      }

      // Should not reach here
      System.exit(-1);
      return -1;
  }

  @Override
  public RelNode visit(LogicalFilter filter) {
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

    List<RexNode> operands = ((RexCall) condition).getOperands();
    // Visit the operands of the filter operator
    Integer filterOrUnionOperator = visitFilterOperands(condition, operands, children.get(0));
    assert filterOrUnionOperator!=-1;
    this.childrenOperators.peek().add(filterOrUnionOperator);

    return filter;
  }

  @Override
  public RelNode visit(LogicalJoin join) {
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

    // Propagate up to parents.
    this.childrenOperators.peek().add(joinOperator);
    return join;
  }

  @Override
  public RelNode visit(LogicalProject project){
    // Add a new level in the stack to store ids of the direct children operators.
    this.childrenOperators.push(new ArrayList<Integer>());

    // Visit children.
    visitChildren(project);

    // Get IDs of generated children.
    ArrayList<Integer> children = this.childrenOperators.pop();
    assert children.size() == 1;

    ArrayList<Integer> cids = new ArrayList<Integer>();
		for(RexNode item : project.getProjects()) {
			cids.add(Integer.parseInt(item.toString().substring(1)));
		}
    int projectOperator = this.generator.AddProjectOperator(children.get(0), cids.stream().mapToInt(i -> i).toArray());
    this.childrenOperators.peek().add(projectOperator);
    return project;
  }

  @Override
  public RelNode visit(LogicalAggregate aggregate){
    // Add a new level in the stack to store ids of the direct children operators.
    this.childrenOperators.push(new ArrayList<Integer>());

    // Visit children.
    visitChildren(aggregate);

    // Get IDs of generated children.
    ArrayList<Integer> children = this.childrenOperators.pop();
    assert children.size() == 1;

    List<Integer> groupCols = aggregate.getGroupSet().toList();
    // We only support a single aggregate function per operator at the moment
    assert aggregate.getAggCallList().size() == 1;
    AggregateCall aggCall = aggregate.getAggCallList().get(0);
    int functionEnum = -1;
    int aggCol = -1;
    switch(aggCall.getAggregation().getKind()){
      case COUNT:
        functionEnum = DataFlowGraphLibrary.COUNT;
        // Count does not have an aggregate column, for safety the data flow
        // operator also does not depend on it.
        assert aggCall.getArgList().size() == 0;
        aggCol = -1;
        break;
      case SUM:
        functionEnum = DataFlowGraphLibrary.SUM;
        assert aggCall.getArgList().size() == 1;
        aggCol = aggCall.getArgList().get(0);
        break;
      default:
        throw new IllegalArgumentException("Invalid aggregate function");
    }

    int aggregateOperator = this.generator.AddAggregateOperator(children.get(0), groupCols.stream().mapToInt(i -> i).toArray(), functionEnum, aggCol);
    this.childrenOperators.peek().add(aggregateOperator);
    return aggregate;
  }
}
