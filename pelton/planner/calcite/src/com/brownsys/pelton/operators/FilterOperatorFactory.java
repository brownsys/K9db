package com.brownsys.pelton.operators;

import com.brownsys.pelton.PlanningContext;
import com.brownsys.pelton.nativelib.DataFlowGraphLibrary;
import com.brownsys.pelton.operators.util.FilterShapeVisitor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

public class FilterOperatorFactory {
  // Allowed expressions that we know how to plan.
  public static final List<SqlKind> FILTER_OPERATIONS =
      Arrays.asList(
          SqlKind.EQUALS,
          SqlKind.NOT_EQUALS,
          SqlKind.LESS_THAN,
          SqlKind.LESS_THAN_OR_EQUAL,
          SqlKind.GREATER_THAN,
          SqlKind.GREATER_THAN_OR_EQUAL,
          SqlKind.IS_NULL,
          SqlKind.IS_NOT_NULL);

  // The context we use to access the native graph generation API.
  private final PlanningContext context;

  public FilterOperatorFactory(PlanningContext context) {
    this.context = context;
  }

  // Create operator(s) that are equivalent to the given filter.
  // For simple filters, this results in a single filter operator.
  // For more sophisticated filters with nested levels of ANDs and ORs, this may
  // result in a similarly nested tree of union and filter operators.
  public int createOperator(LogicalFilter filter, ArrayList<Integer> children) {
    assert children.size() == 1;

    RexNode condition = filter.getCondition();
    assert condition instanceof RexCall;

    Boolean goodShape = condition.accept(new FilterShapeVisitor());
    if (goodShape == null || !goodShape) {
      throw new IllegalArgumentException("Bad filter shape: unknown construct!");
    }

    // Visit the operands of the filter operator
    return this.analyzeCondition((RexCall) condition, children.get(0));
  }

  // Analyze a generic clause: could be a simple condition (e.g. x == y) or a tree
  // of expressions (e.g. (x == y OR y == z) AND ...).
  private int analyzeCondition(RexCall condition, int inputOperator) {
    if (condition.isA(SqlKind.AND)) {
      return this.analyzeAnd(condition, inputOperator);
    } else if (condition.isA(SqlKind.OR)) {
      return this.analyzeOr(condition, inputOperator);
    } else if (condition.isA(FILTER_OPERATIONS)) {
      List<RexNode> filters = new ArrayList<RexNode>();
      filters.add(condition);
      return this.analyzeDirectFilter(filters, inputOperator);
    } else {
      throw new IllegalArgumentException("Illegal filter condition kind " + condition.getKind());
    }
  }

  // Generate a Union operator connected to all OR clauses.
  private int analyzeOr(RexCall condition, int inputOperator) {
    List<RexNode> operands = condition.getOperands();
    int[] childrenOperators = new int[operands.size()];
    for (int i = 0; i < operands.size(); i++) {
      RexNode operand = operands.get(i);
      assert operand instanceof RexCall;
      childrenOperators[i] = this.analyzeCondition((RexCall) operand, inputOperator);
    }

    return this.context.getGenerator().AddUnionOperator(childrenOperators);
  }

  // Ideally would generate an intersection operator connected to all AND clauses.
  // We do not support intersection, instead we assume this can only contain simple
  // directly expressions as clauses, and can thus be represented as a single filter.
  private int analyzeAnd(RexCall condition, int inputOperator) {
    // And can only contain either other ands or direct expressions.
    boolean allDirectExpression = true;
    List<RexNode> operands = condition.getOperands();
    for (RexNode operand : operands) {
      if (!operand.isA(FILTER_OPERATIONS)) {
        allDirectExpression = false;
      }
    }

    if (allDirectExpression) {
      return this.analyzeDirectFilter(operands, inputOperator);
    } else {
      // GoodShapeVisitor guarantees we will never get here.
      assert false;

      int[] childrenOperators = new int[operands.size()];
      for (int i = 0; i < operands.size(); i++) {
        RexNode operand = operands.get(i);
        assert operand instanceof RexCall;
        childrenOperators[i] = this.analyzeCondition((RexCall) operand, inputOperator);
      }

      // TODO(babman): Replace this with intersection if we need to support this.
      return this.context.getGenerator().AddUnionOperator(childrenOperators);
    }
  }

  // Generate a filter operator that filters according to the given (simple expression) operands.
  // Operands cannot have ANDs, ORs, or complex nesting. They must each match an option from
  // FILTER_OPERATIONS.
  // Generated operator is equivalent to AND(operands).
  private int analyzeDirectFilter(List<RexNode> operands, int inputOperator) {
    int filterOperator = this.context.getGenerator().AddFilterOperator(inputOperator);
    for (RexNode operand : operands) {
      this.addFilterOperation((RexCall) operand, filterOperator);
    }
    return filterOperator;
  }

  // Add a single simple condition to an existing filter operator.
  // Operand must match one of the options in FILTER_OPERATIONS.
  private void addFilterOperation(RexCall condition, int filterOperator) {
    // Determine the condition operation.
    int operationEnum = -1;
    List<RexNode> operands = condition.getOperands();
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
      case IS_NULL:
        operationEnum = DataFlowGraphLibrary.IS_NULL;
        break;
      case IS_NOT_NULL:
        operationEnum = DataFlowGraphLibrary.IS_NOT_NULL;
        break;
      default:
        assert false;
    }

    if (operands.size() == 1) {
      this.addUnaryCondition(operands, operationEnum, filterOperator);
    } else if (operands.size() == 2) {
      if (operands.get(0) instanceof RexInputRef && operands.get(1) instanceof RexInputRef) {
        this.addColumnBinaryCondition(
            (RexInputRef) operands.get(0),
            (RexInputRef) operands.get(1),
            operationEnum,
            filterOperator);
      } else if (operands.get(0) instanceof RexInputRef) {
        this.addLeftValueBinaryCondition(
            (RexInputRef) operands.get(0), operands.get(1), operationEnum, filterOperator);
      } else if (operands.get(1) instanceof RexInputRef) {
        this.addRightValueBinaryCondition(
            operands.get(0), (RexInputRef) operands.get(1), operationEnum, filterOperator);
      } else {
        throw new IllegalArgumentException("Filter condition must be over at least one column");
      }
    } else {
      throw new IllegalArgumentException("Invalid arty for filter condition " + operands.size());
    }
  }

  private void addUnaryCondition(List<RexNode> operands, int operationEnum, int filterOperator) {
    // Unary conditions must be over a column.
    assert operands.get(0) instanceof RexInputRef;
    assert operationEnum == DataFlowGraphLibrary.IS_NULL
        || operationEnum == DataFlowGraphLibrary.IS_NOT_NULL;

    RexInputRef input = (RexInputRef) operands.get(0);
    int columnId = this.context.getPeltonIndex(input.getIndex());
    this.context.getGenerator().AddFilterOperationNull(filterOperator, columnId, operationEnum);
  }

  private void addColumnBinaryCondition(
      RexInputRef left, RexInputRef right, int operationEnum, int filterOperator) {
    int leftId = this.context.getPeltonIndex(left.getIndex());
    int rightId = this.context.getPeltonIndex(right.getIndex());
    // this.context.getGenerator().AddFilterOperationColumn(filterOperator, leftId, operationEnum,
    // rightId);
  }

  private void addLeftValueBinaryCondition(
      RexInputRef left, RexNode right, int operationEnum, int filterOperator) {
    assert right instanceof RexLiteral || right instanceof RexDynamicParam;
    ;
    int columnId = this.context.getPeltonIndex(left.getIndex());

    // Handle parameters (`?` in query)
    if (right instanceof RexDynamicParam) {
      this.context.addMatViewKey(columnId);
      return;
    }

    // Determine the value type.
    RexLiteral value = (RexLiteral) right;
    switch (value.getTypeName()) {
      case DECIMAL:
      case INTEGER:
        this.context
            .getGenerator()
            .AddFilterOperationSigned(
                filterOperator, RexLiteral.intValue(value), columnId, operationEnum);
        break;

      case VARCHAR:
      case CHAR:
        this.context
            .getGenerator()
            .AddFilterOperationString(
                filterOperator, RexLiteral.stringValue(value), columnId, operationEnum);
        break;

      case NULL:
        switch (operationEnum) {
          case DataFlowGraphLibrary.EQUAL:
            operationEnum = DataFlowGraphLibrary.IS_NULL;
            break;
          case DataFlowGraphLibrary.NOT_EQUAL:
            operationEnum = DataFlowGraphLibrary.IS_NOT_NULL;
            break;
          default:
            throw new IllegalArgumentException("Illegal operation on null");
        }
        this.context.getGenerator().AddFilterOperationNull(filterOperator, columnId, operationEnum);
        break;

      default:
        throw new IllegalArgumentException(
            "Invalid literal type in filter: " + value.getTypeName());
    }
  }

  private void addRightValueBinaryCondition(
      RexNode left, RexInputRef right, int operationEnum, int filterOperator) {
    switch (operationEnum) {
        // Invert left and right.
      case DataFlowGraphLibrary.LESS_THAN:
        this.addLeftValueBinaryCondition(
            right, left, DataFlowGraphLibrary.GREATER_THAN_OR_EQUAL, filterOperator);
        break;
      case DataFlowGraphLibrary.LESS_THAN_OR_EQUAL:
        this.addLeftValueBinaryCondition(
            right, left, DataFlowGraphLibrary.GREATER_THAN, filterOperator);
        break;
      case DataFlowGraphLibrary.GREATER_THAN:
        this.addLeftValueBinaryCondition(
            right, left, DataFlowGraphLibrary.LESS_THAN_OR_EQUAL, filterOperator);
        break;
      case DataFlowGraphLibrary.GREATER_THAN_OR_EQUAL:
        this.addLeftValueBinaryCondition(
            right, left, DataFlowGraphLibrary.LESS_THAN, filterOperator);
        break;
        // Symmetric operations.
      case DataFlowGraphLibrary.EQUAL:
      case DataFlowGraphLibrary.NOT_EQUAL:
        this.addLeftValueBinaryCondition(right, left, operationEnum, filterOperator);
        break;
        // Unreachable.
      default:
        throw new IllegalArgumentException("Illegal binary value operation " + operationEnum);
    }
  }
}
