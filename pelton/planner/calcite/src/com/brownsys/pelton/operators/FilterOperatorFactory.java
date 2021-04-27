package com.brownsys.pelton.operators;

import com.brownsys.pelton.PlanningContext;
import com.brownsys.pelton.nativelib.DataFlowGraphLibrary;
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
  private static final List<SqlKind> FILTER_OPERATIONS =
      Arrays.asList(
          SqlKind.EQUALS,
          SqlKind.NOT_EQUALS,
          SqlKind.LESS_THAN,
          SqlKind.LESS_THAN_OR_EQUAL,
          SqlKind.GREATER_THAN,
          SqlKind.GREATER_THAN_OR_EQUAL,
          SqlKind.IS_NULL,
          SqlKind.IS_NOT_NULL);

  private final PlanningContext context;

  public FilterOperatorFactory(PlanningContext context) {
    this.context = context;
  }

  private void addFilterOperation(int filterOperator, RexNode condition, List<RexNode> operands) {
    // Determine the condition operation.
    int operationEnum = -1;
    boolean is_unary = false;
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
        is_unary = true;
        break;
      case IS_NOT_NULL:
        operationEnum = DataFlowGraphLibrary.IS_NOT_NULL;
        is_unary = true;
        break;
      default:
        assert false;
    }

    if (operands.size() == 1) {
      if (!is_unary) {
        throw new IllegalArgumentException("Too few operands to filter!");
      }

      assert operands.get(0) instanceof RexInputRef;
      RexInputRef input = (RexInputRef) operands.get(0);
      int columnId = input.getIndex();
      this.context.getGenerator().AddFilterOperationNull(filterOperator, columnId, operationEnum);
      return;

    } else {
      assert operands.size() == 2;
      assert operands.get(0) instanceof RexInputRef || operands.get(1) instanceof RexInputRef;
      assert operands.get(0) instanceof RexLiteral
          || operands.get(1) instanceof RexLiteral
          || operands.get(0) instanceof RexDynamicParam
          || operands.get(1) instanceof RexDynamicParam;
      // Get the input and the value expressions.
      int inputIndex = operands.get(0) instanceof RexInputRef ? 0 : 1;
      int valueIndex = (inputIndex + 1) % 2;
      RexInputRef input = (RexInputRef) operands.get(inputIndex);

      // Handle parameters (`?` in query)
      if (operands.get(valueIndex) instanceof RexDynamicParam
          && !(operands.get(valueIndex) instanceof RexLiteral)) {
        this.context.addMatViewKey(valueIndex);
        return;
      }

      RexLiteral value = (RexLiteral) operands.get(valueIndex);

      // Determine the value type.
      int columnId = input.getIndex();
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
              .AddFilterOperation(
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
          this.context
              .getGenerator()
              .AddFilterOperationNull(filterOperator, columnId, operationEnum);
          break;

        default:
          throw new IllegalArgumentException(
              "Invalid literal type in filter: " + value.getTypeName());
      }
    }
  }

  private int visitFilterOperands(
      RexNode condition, List<RexNode> operands, Integer deepestFilterParent) {
    if (condition.isA(SqlKind.AND)) {
      // If there are >1 nested conditions then connect them via a new union
      // operator else link the single child operator directly.
      // NOTE(Ishan): There is room for optimization here regarding chaining of
      // operators in certain scenarios instead of using a union operator,
      // but for lobesters' queries it won't make a difference.

      // Operands inserted in the following list are not nested
      List<RexNode> operations = new ArrayList<RexNode>();
      List<Integer> nestedOperators = new ArrayList<Integer>();
      for (RexNode operand : operands) {
        if (operand.isA(SqlKind.OR)) {
          nestedOperators.add(
              visitFilterOperands(operand, ((RexCall) operand).getOperands(), deepestFilterParent));
        } else if (operand.isA(SqlKind.AND)) {
          nestedOperators.add(
              visitFilterOperands(operand, ((RexCall) operand).getOperands(), deepestFilterParent));
        } else {
          operations.add(operand);
        }
      }
      // We currently do not support 0 "base" operations, for example AND(OR, OR)
      assert operations.size() != 0;
      // Decide on parent(w.r.t the dataflow) of this filter operator
      int filterOperator;
      if (nestedOperators.size() == 0) {
        // Deepest filter operator with no further nested conditions
        filterOperator = this.context.getGenerator().AddFilterOperator(deepestFilterParent);
      } else if (nestedOperators.size() == 1) {
        // Link the nested operator directly.
        filterOperator = this.context.getGenerator().AddFilterOperator(nestedOperators.get(0));
      } else {
        // Link the nested operators via a union and link the current operator
        // with that union.
        int[] primitiveArray = nestedOperators.stream().mapToInt(i -> i).toArray();
        int unionOperator = this.context.getGenerator().AddUnionOperator(primitiveArray);
        filterOperator = this.context.getGenerator().AddFilterOperator(unionOperator);
      }
      // Add operations to the newly generated operator
      for (RexNode operation : operations) {
        addFilterOperation(filterOperator, operation, ((RexCall) operation).getOperands());
      }
      return filterOperator;
    } else if (condition.isA(SqlKind.OR)) {
      // We are following a pure union based  approach. Treat all the
      // operands as children of the union operator.
      // It does not matter whether the operand is nested or not.
      List<Integer> unionParents = new ArrayList<Integer>();
      for (RexNode operand : operands) {
        unionParents.add(
            visitFilterOperands(operand, ((RexCall) operand).getOperands(), deepestFilterParent));
      }
      // Add union operator
      int[] primitiveArray = unionParents.stream().mapToInt(i -> i).toArray();
      int unionOperator = this.context.getGenerator().AddUnionOperator(primitiveArray);
      return unionOperator;
    } else if (condition.isA(FILTER_OPERATIONS)) {
      // Will only reach here if
      // 1. Either the core filter operator does not contain any nested conditions,
      // 2. or @param condition is part of the OR
      // condition. In this scenario, the constructed filter operator (a default AND
      // filter operator) will be the leaf. Hence it's parent will be
      // @deepestFilterParent
      int filterOperator = this.context.getGenerator().AddFilterOperator(deepestFilterParent);
      addFilterOperation(filterOperator, condition, ((RexCall) condition).getOperands());
      return filterOperator;
    }

    // Should not reach here
    System.exit(-1);
    return -1;
  }

  public int createOperator(LogicalFilter filter, ArrayList<Integer> children) {
    assert children.size() == 1;

    RexNode condition = filter.getCondition();
    assert condition instanceof RexCall;
    assert condition.isA(SqlKind.AND) || condition.isA(FILTER_OPERATIONS);

    List<RexNode> operands = ((RexCall) condition).getOperands();
    // Visit the operands of the filter operator
    return this.visitFilterOperands(condition, operands, children.get(0));
  }
}
