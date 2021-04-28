package com.brownsys.pelton.operators;

import com.brownsys.pelton.PlanningContext;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

public class JoinOperatorFactory {
  private final PlanningContext context;

  public JoinOperatorFactory(PlanningContext context) {
    this.context = context;
  }

  public int createOperator(LogicalJoin join, ArrayList<Integer> children) {
    // Find the size of the schema of the left relation.
    int leftCalciteCount = join.getLeft().getRowType().getFieldCount();
    int leftPeltonCount = 0;
    HashSet<Integer> dups = new HashSet<>();
    for (int i = 0; i < leftCalciteCount; i++) {
      int p = this.context.getPeltonIndex(i);
      if (!dups.contains(p)) {
        dups.add(p);
        leftPeltonCount++;
      }
    }

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

    int leftCalciteIndex = ((RexInputRef) leftCondition).getIndex();
    int rightClaciteIndex = ((RexInputRef) rightCondition).getIndex();
    int leftPeltonIndex = this.context.getPeltonIndex(leftCalciteIndex);
    int rightPeltonIndex = this.context.getPeltonIndex(rightClaciteIndex);
    this.context.setDuplicate(rightPeltonIndex, leftPeltonIndex);
    rightPeltonIndex -= leftPeltonCount;

    // Set up all join operator parameters.
    int leftInput = children.get(0);
    int rightInput = children.get(1);

    // Find the right join operator per join type.
    int joinOperator = -1;
    switch (join.getJoinType()) {
      case INNER:
        joinOperator =
            this.context
                .getGenerator()
                .AddEquiJoinOperator(leftInput, rightInput, leftPeltonIndex, rightPeltonIndex);
        break;
      case ANTI:
      case FULL:
      case LEFT:
      case RIGHT:
      case SEMI:
        throw new IllegalArgumentException("Unsupported join type " + join.getJoinType());
    }

    return joinOperator;
  }
}
