package com.brownsys.pelton.operators;

import com.brownsys.pelton.PlanningContext;
import com.brownsys.pelton.nativelib.DataFlowGraphLibrary;
import com.brownsys.pelton.util.RexArithmeticCollector;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
  private final ProjectOperatorFactory projectFactory;
  private final HashMap<RexNode, Integer> arithmeticExpressionToProjectedColumn;

  public FilterOperatorFactory(PlanningContext context) {
    this.context = context;
    this.projectFactory = new ProjectOperatorFactory(context);
    this.arithmeticExpressionToProjectedColumn = new HashMap<RexNode, Integer>();
  }

  // Create operator(s) that are equivalent to the given filter.
  // For simple filters, this results in a single filter operator.
  // For more sophisticated filters with nested levels of ANDs and ORs, this may
  // result in a similarly nested tree of union and filter operators.
  public int createOperator(LogicalFilter filter, ArrayList<Integer> children) {
    assert children.size() == 1;
    int inputOperator = children.get(0);

    RexNode condition = filter.getCondition();
    assert condition instanceof RexCall;

    // If the filter has arithmetic expressions (e.g. col + 10 < something)
    // or (col - col == something), we support these expressions by first
    // introducing them as projections with tmp column names, then using these
    // columns in their place during filtering. Finally, after the filter a new
    // projection is carried out to remove these tmp columns.
    RexArithmeticCollector arithmeticVisitor = new RexArithmeticCollector();
    boolean hasArithmetic = condition.accept(arithmeticVisitor);
    int tmpColumnCount = 0;
    int originalColumnCount = this.context.getPeltonColumnCount();
    if (hasArithmetic) {
      inputOperator = this.context.getGenerator().AddProjectOperator(inputOperator);
      for (int i = 0; i < originalColumnCount; i++) {
        // "" --> keep column name.
        this.projectFactory.addColumnProjection("", i, inputOperator);
      }
      List<RexCall> arithmetics = arithmeticVisitor.getArithmeticNodes();
      for (RexCall r : arithmetics) {
        int projectedColumnIndex = originalColumnCount + tmpColumnCount++;
        this.projectArithmeticExpression(r, projectedColumnIndex, inputOperator);
        this.arithmeticExpressionToProjectedColumn.put(r, projectedColumnIndex);
      }
    }

    // Visit the operands of the filter operator
    int outputOperator = this.analyzeCondition((RexCall) condition, inputOperator);

    // Remove tmp columns.
    if (hasArithmetic) {
      assert outputOperator != -1;
      outputOperator = this.context.getGenerator().AddProjectOperator(outputOperator);
      for (int i = 0; i < originalColumnCount; i++) {
        this.projectFactory.addColumnProjection("", i, outputOperator);
      }
    }

    return outputOperator;
  }

  /*
   * Section for recursively traversing AND/OR filter tree.
   */
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
      assert childrenOperators[i] != -1;
    }

    return this.context.getGenerator().AddUnionOperator(childrenOperators);
  }

  // Generate a filter for this AND condition. Nested ORs are supported.
  private int analyzeAnd(RexCall condition, int inputOperator) {
    // And can only contain either direct expressions or exactly a single OR.
    ArrayList<RexNode> directExpressions = new ArrayList<RexNode>();
    for (RexNode operand : condition.getOperands()) {
      if (operand.isA(FILTER_OPERATIONS)) {
        directExpressions.add(operand);
      } else if (operand.isA(SqlKind.OR)) {
        // If we have an OR: we process the OR first and generate its operators.
        // The OR's output union operator becomes the input to our Filter.
        // If this And condition contains other ORs, they are chained after this OR.
        inputOperator = this.analyzeOr((RexCall) operand, inputOperator);
        assert inputOperator != -1;
      } else {
        throw new IllegalArgumentException("Found illegal " + operand.getKind() + " in AND");
      }
    }

    if (directExpressions.size() > 0) {
      int directFilter = this.analyzeDirectFilter(directExpressions, inputOperator);
      if (directFilter > -1) {
        return directFilter;
      }
    }
    return inputOperator;
  }

  /*
   * Section for parsing and creating a filter operator for a single level of
   * the filter tree.
   */
  // Generate a filter operator that filters according to the given (simple expression) operands.
  // Operands cannot have ANDs, ORs, or complex nesting. They must each match an option from
  // FILTER_OPERATIONS.
  // Generated operator is equivalent to AND(operands).
  private int analyzeDirectFilter(List<RexNode> operands, int inputOperator) {
    // Do not create a filter operator in pelton if this filter only contains
    // conditions with RexDynamicParam. Else a query like "SELECT * from
    // submissions WHERE ID = ?" would end up creating a no-op filter since
    // the where condition is only used to key the matview.
    boolean isNoop = analyzeNoop(operands);
    if (isNoop) {
      return -1;
    }
    // End of no-op check

    int filterOperator = this.context.getGenerator().AddFilterOperator(inputOperator);
    for (RexNode operand : operands) {
      if (operand.isA(FILTER_OPERATIONS)) {
        this.addFilterOperation((RexCall) operand, filterOperator);
      }
    }
    return filterOperator;
  }

  private boolean analyzeNoop(List<RexNode> conditions) {
    // This filter operator will result in a no-op if all the conditions are
    // of type RexDynamicParam (i.e. contain a "?")
    for (RexNode condition : conditions) {
      List<RexNode> operands = ((RexCall) condition).getOperands();
      if (operands.size() == 1) {
        // Cannot contain a "?"
        return false;
      } else {
        if (!(operands.get(0) instanceof RexDynamicParam)
            && !(operands.get(1) instanceof RexDynamicParam)) {
          return false;
        }
      }
    }
    // If reached here then it implies that all conditions are binary and each one
    // contains at least one "?". Hence this filter will be a no-op.

    // Do not create an exchange operator, simply specify keys that the matview
    // should be keyed on.
    for (RexNode condition : conditions) {
      int operationEnum = getOperationEnum((RexCall) condition);
      List<RexNode> operands = ((RexCall) condition).getOperands();
      if (operands.get(0) instanceof RexDynamicParam) {
        this.addQuestionMarkCondition(operands.get(1), operationEnum);
      } else if (operands.get(1) instanceof RexDynamicParam) {
        this.addQuestionMarkCondition(operands.get(0), operationEnum);
      } else {
        // Should not reach here
        assert false;
      }
    }
    return true;
  }

  private int getOperationEnum(RexCall condition) {
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
      case IS_NULL:
        operationEnum = DataFlowGraphLibrary.IS_NULL;
        break;
      case IS_NOT_NULL:
        operationEnum = DataFlowGraphLibrary.IS_NOT_NULL;
        break;
      default:
        assert false;
    }
    return operationEnum;
  }

  // Add a single simple condition to an existing filter operator.
  // Operand must match one of the options in FILTER_OPERATIONS.
  private void addFilterOperation(RexCall condition, int filterOperator) {
    // Determine the condition operation.
    int operationEnum = getOperationEnum(condition);
    List<RexNode> operands = condition.getOperands();
    if (operands.size() == 1) {
      this.addUnaryCondition(operands, operationEnum, filterOperator);
    } else if (operands.size() == 2) {
      boolean leftArith = this.arithmeticExpressionToProjectedColumn.containsKey(operands.get(0));
      boolean rightArith = this.arithmeticExpressionToProjectedColumn.containsKey(operands.get(1));

      // Expressions with a ? parameter.
      if (operands.get(0) instanceof RexDynamicParam) {
        this.addQuestionMarkCondition(operands.get(1), operationEnum);
      } else if (operands.get(1) instanceof RexDynamicParam) {
        this.addQuestionMarkCondition(operands.get(0), operationEnum);
      }
      // Expression involving some arithmetic expressions.
      else if (leftArith && rightArith) {
        // Both arithmetic.
        int leftId = this.arithmeticExpressionToProjectedColumn.get(operands.get(0));
        int rightId = this.arithmeticExpressionToProjectedColumn.get(operands.get(1));
        this.addColumnBinaryCondition(leftId, rightId, operationEnum, filterOperator);
      } else if (leftArith && operands.get(1) instanceof RexInputRef) {
        // Arithmetic, Column.
        int leftId = this.arithmeticExpressionToProjectedColumn.get(operands.get(0));
        int rightId = this.context.getPeltonIndex(((RexInputRef) operands.get(1)).getIndex());
        this.addColumnBinaryCondition(leftId, rightId, operationEnum, filterOperator);
      } else if (leftArith && operands.get(1) instanceof RexLiteral) {
        // Arithmetic, Literal.
        int leftId = this.arithmeticExpressionToProjectedColumn.get(operands.get(0));
        this.addLiteralBinaryCondition(
            leftId, (RexLiteral) operands.get(1), operationEnum, filterOperator);
      } else if (rightArith && operands.get(0) instanceof RexInputRef) {
        // Column, Arithmetic.
        int leftId = this.context.getPeltonIndex(((RexInputRef) operands.get(0)).getIndex());
        int rightId = this.arithmeticExpressionToProjectedColumn.get(operands.get(1));
        this.addColumnBinaryCondition(leftId, rightId, operationEnum, filterOperator);
      } else if (rightArith && operands.get(0) instanceof RexLiteral) {
        // Literal, Arithmetic.
        int rightId = this.arithmeticExpressionToProjectedColumn.get(operands.get(1));
        this.addLiteralBinaryCondition(
            (RexLiteral) operands.get(0), rightId, operationEnum, filterOperator);
      }
      // Expression on two columns.
      else if (operands.get(0) instanceof RexInputRef && operands.get(1) instanceof RexInputRef) {
        int leftId = this.context.getPeltonIndex(((RexInputRef) operands.get(0)).getIndex());
        int rightId = this.context.getPeltonIndex(((RexInputRef) operands.get(1)).getIndex());
        this.addColumnBinaryCondition(leftId, rightId, operationEnum, filterOperator);
      }
      // Expression on a column and literal
      else if (operands.get(0) instanceof RexInputRef && operands.get(1) instanceof RexLiteral) {
        int leftId = this.context.getPeltonIndex(((RexInputRef) operands.get(0)).getIndex());
        this.addLiteralBinaryCondition(
            leftId, (RexLiteral) operands.get(1), operationEnum, filterOperator);
      } else if (operands.get(1) instanceof RexInputRef && operands.get(0) instanceof RexLiteral) {
        int rightId = this.context.getPeltonIndex(((RexInputRef) operands.get(1)).getIndex());
        this.addLiteralBinaryCondition(
            (RexLiteral) operands.get(0), rightId, operationEnum, filterOperator);
      }
      // Something else: not supported!
      else {
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

  private void addQuestionMarkCondition(RexNode otherOperand, int operationEnum) {
    if (operationEnum != DataFlowGraphLibrary.EQUAL) {
      throw new IllegalArgumentException("Can only use ? in an equality condition");
    }
    if (!(otherOperand instanceof RexInputRef)) {
      throw new IllegalArgumentException("Cannot compare ? to a non-column expression");
    }
    int columnId = this.context.getPeltonIndex(((RexInputRef) otherOperand).getIndex());
    this.context.addMatViewKey(columnId);
  }

  private void addColumnBinaryCondition(
      int leftId, int rightId, int operationEnum, int filterOperator) {
    this.context
        .getGenerator()
        .AddFilterOperationColumn(filterOperator, leftId, rightId, operationEnum);
  }

  private void addLiteralBinaryCondition(
      int leftColumnId, RexLiteral right, int operationEnum, int filterOperator) {
    // Determine the value type.
    switch (right.getTypeName()) {
      case DECIMAL:
      case INTEGER:
        this.context
            .getGenerator()
            .AddFilterOperationInt(
                filterOperator, RexLiteral.intValue(right), leftColumnId, operationEnum);
        break;

      case VARCHAR:
      case CHAR:
        this.context
            .getGenerator()
            .AddFilterOperationString(
                filterOperator, RexLiteral.stringValue(right), leftColumnId, operationEnum);
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
        this.context
            .getGenerator()
            .AddFilterOperationNull(filterOperator, leftColumnId, operationEnum);
        break;

      default:
        throw new IllegalArgumentException(
            "Invalid literal type in filter: " + right.getTypeName());
    }
  }

  private void addLiteralBinaryCondition(
      RexLiteral left, int right, int operationEnum, int filterOperator) {
    switch (operationEnum) {
        // Invert left and right.
      case DataFlowGraphLibrary.LESS_THAN:
        this.addLiteralBinaryCondition(
            right, left, DataFlowGraphLibrary.GREATER_THAN, filterOperator);
        break;
      case DataFlowGraphLibrary.LESS_THAN_OR_EQUAL:
        this.addLiteralBinaryCondition(
            right, left, DataFlowGraphLibrary.GREATER_THAN_OR_EQUAL, filterOperator);
        break;
      case DataFlowGraphLibrary.GREATER_THAN:
        this.addLiteralBinaryCondition(right, left, DataFlowGraphLibrary.LESS_THAN, filterOperator);
        break;
      case DataFlowGraphLibrary.GREATER_THAN_OR_EQUAL:
        this.addLiteralBinaryCondition(
            right, left, DataFlowGraphLibrary.LESS_THAN_OR_EQUAL, filterOperator);
        break;
        // Symmetric operations.
      case DataFlowGraphLibrary.EQUAL:
      case DataFlowGraphLibrary.NOT_EQUAL:
        this.addLiteralBinaryCondition(right, left, operationEnum, filterOperator);
        break;
        // Unreachable.
      default:
        throw new IllegalArgumentException("Illegal binary value operation " + operationEnum);
    }
  }

  /*
   * Section for adding appropriate projections to compute arithmetic expressions ahead of
   * the filter.
   */
  private void projectArithmeticExpression(
      RexCall expression, int peltonIndex, int projectOperator) {
    String name = "_PELTON_TMP_" + (peltonIndex + 1);
    int arithmeticEnum = -1;
    switch (expression.getKind()) {
      case PLUS:
        arithmeticEnum = DataFlowGraphLibrary.PLUS;
        break;
      case MINUS:
        arithmeticEnum = DataFlowGraphLibrary.MINUS;
        break;
      default:
        throw new IllegalArgumentException("Unsupported arithmetic expression in filter");
    }
    this.projectFactory.addArithmeticProjection(
        name, expression.getOperands(), arithmeticEnum, projectOperator);
  }
}
