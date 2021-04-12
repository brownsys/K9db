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
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
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
  private final Hashtable<Integer, ArrayList<Integer>> filterOperators;

  public PhysicalPlanVisitor(DataFlowGraphLibrary.DataFlowGraphGenerator generator) {
    this.generator = generator;
    this.childrenOperators = new Stack<ArrayList<Integer>>();
    this.tableToInputOperator = new Hashtable<String, Integer>();
    this.filterOperators = new Hashtable<Integer, ArrayList<Integer>>();
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

  private void visitFilterOperands(Integer level, RexNode condition, List<RexNode> operands) {
      List<RexNode> operations = new ArrayList<RexNode>();
      if(condition.isA(SqlKind.AND) || condition.isA(SqlKind.OR)){
        for(RexNode operand : operands){
          if(operand.isA(SqlKind.AND) || operand.isA(SqlKind.OR)){
            visitFilterOperands(level+1, operand, ((RexCall) condition).getOperands());
          } else{
            operations.add(operand);
          }
        }
      }

      System.out.println("[INFO] Adding operations to filter: " + operations);

      // Will reach here in two cases:
      // 1. Either all of the operands of @param condition are of kind
      // FILTER_OPERATIONS
      // 2. Or all of the nested conditions have been visited.

      // Create a new filter operator (parent for this operator will be added
      // later, once all the operands have been visited)
      // TODO(Ishan): Update this to accound for AND vs OR filters
      int filterOperator = this.generator.AddFilterOperator();
      // Add operations to the operator
      for(RexNode operation: operations){
        addFilterOperation(filterOperator, condition, ((RexCall) operation).getOperands());
      }
      if(!this.filterOperators.containsKey(level)){
        ArrayList<Integer> filterOps = new ArrayList<Integer>();
        filterOps.add(filterOperator);
        this.filterOperators.put(level, filterOps);
      } else{
        ArrayList<Integer> filterOps = this.filterOperators.get(level);
        filterOps.add(filterOperator);
        this.filterOperators.replace(level, filterOps);
      }

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
    if(condition.isA(FILTER_OPERATIONS)){
      // A basic filter operator, does not contain AND/OR
      int filterOperator = this.generator.AddFilterOperator(children.get(0));
      addFilterOperation(filterOperator, condition, operands);
      this.childrenOperators.peek().add(filterOperator);
    } else{
      this.visitFilterOperands(0, condition, operands);
      // Chain the various filter operators in the stack
      // 1. Link the deepest nested filter operator to the child of @param filter
      // operator in the plan
      Integer maxLevel = -1;
      for(Integer level : this.filterOperators.keySet()){
        if(level > maxLevel){
          maxLevel = level;
        }
      }
      // Picking the last one (any one is fine since they are at the same level)
      System.out.println(this.filterOperators);
      Integer deepestFilter = this.filterOperators.get(maxLevel).get(this.filterOperators.get(maxLevel).size()-1);
      // this.filterOperators.replace(maxlevel, deepestFilters);
      this.generator.AddFilterOperatorParent(deepestFilter, children.get(0));

      // 2. Link the filter operators in between, start from maxLevel and go to
      // the topmost filter
      // Flatten the hastable into an array
      ArrayList<Integer> flatFilterOperators = new ArrayList<Integer>();
      for(Integer i = 0; i<=maxLevel; i++){
        flatFilterOperators.addAll(this.filterOperators.get(i));
      }
      for(Integer i=0; i< flatFilterOperators.size()-2; i++){
        this.generator.AddFilterOperatorParent(flatFilterOperators.get(i), flatFilterOperators.get(i+1));
      }
      // 3. Link the topmost filter operator to the parent of @param filter
      // operator in the plan. previousFilterIndex from above represents topmost
      // filter operator.
      this.childrenOperators.peek().add(flatFilterOperators.get(0));
    }

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
}
