package com.brownsys.k9db.rules;

import com.brownsys.k9db.util.RexInputRefCollector;
import com.brownsys.k9db.util.RexInputRefOffsetter;
import java.util.HashSet;
import java.util.Stack;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;

// https://calcite.apache.org/javadocAggregate/org/apache/calcite/plan/RelRule.html
public class FilterPastJoin extends RelRule<FilterPastJoin.Config> {
  // By making this class private, we ensure we can only create it using
  // FilterPastJoin.Config#toRule(), which ignores the configuration and
  // uses the default ones.
  private FilterPastJoin(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    // Must not mutate the graph, instead register the transformation in the context.
    final LogicalFilter filter = call.rel(0);
    final LogicalJoin join = call.rel(1);

    // Get all the conditions (e.g. terms separated by top level AND) from filter.
    HashSet<RexNode> conditions = this.getConditions(filter);
    if (conditions.isEmpty()) {
      return;
    }

    // Filters pushed to the right operate need to have the column indices they refer to
    // be shifted by the size of the left input schema.
    int leftCols = join.getLeft().getRowType().getFieldCount();
    RexInputRefOffsetter offsetter = new RexInputRefOffsetter(-1 * leftCols);

    // Separate conditions that refer to a single input only from shared conditions.
    HashSet<RexNode> leftConditions = new HashSet<RexNode>();
    HashSet<RexNode> rightConditions = new HashSet<RexNode>();
    HashSet<RexNode> remainingConditions = new HashSet<RexNode>();
    for (RexNode condition : conditions) {
      int direction = this.pushablePastJoin(condition, join);
      if (direction == -1) {
        leftConditions.add(condition);
      } else if (direction == 1) {
        rightConditions.add(condition.accept(offsetter));
      } else {
        remainingConditions.add(condition);
      }
    }

    // Nothing can be pushed.
    if (leftConditions.isEmpty() && rightConditions.isEmpty()) {
      return;
    }

    // Push the conditions to the left or right when appropriate.
    // The join operator is unchanged, the final filter operator
    // consists only if conditions that could not be pushed.
    // P.S.: .filter(<empty>) does nothin.
    final RelBuilder relBuilder = call.builder();
    relBuilder
        .push(join.getLeft())
        .filter(leftConditions)
        .push(join.getRight())
        .filter(rightConditions)
        .join(join.getJoinType(), join.getCondition())
        .filter(remainingConditions);

    call.transformTo(relBuilder.build());
  }

  // Get all the separate conditions checked by the filter.
  // Conditions are separated by top-level AND.
  // For example:
  // C1 -> [C1]
  // C1 AND C2 AND C3  -> [C1, C2, C3]
  // C1 AND (C2 AND C3) -> [C1, C2, C3]
  // C1 AND (C2 OR C3) -> [C1, (C2 OR C3)]
  // C1 OR C2 -> [C1 OR C2]
  // C1 AND (C2 OR (C3 AND C4)) -> [C1, (C2 OR (C3 AND C4))]
  private HashSet<RexNode> getConditions(final LogicalFilter filter) {
    HashSet<RexNode> result = new HashSet<RexNode>();

    Stack<RexNode> dfs = new Stack<RexNode>();
    dfs.push(filter.getCondition());
    while (!dfs.isEmpty()) {
      RexNode term = dfs.pop();
      if (term.isA(SqlKind.AND)) {
        for (RexNode nestedTerm : ((RexCall) term).getOperands()) {
          dfs.push(nestedTerm);
        }
      } else {
        result.add(term);
      }
    }

    return result;
  }

  // Determines if the condition can be pushed past the join.
  // Returns -1 if the condition goes to the left input, 1 if it goes to the
  // right and 0 if it cannot be pushed.
  // A condition cannot be pushed if it refers to columns from both tables,
  // or if it refers to columns from an input side opposite to the direction
  // of the join (if outer join).
  private int pushablePastJoin(RexNode condition, final LogicalJoin join) {
    // Determines which direction can be pushed to based on join type.
    boolean canPushLeft = false;
    boolean canPushRight = false;
    switch (join.getJoinType()) {
      case FULL:
        // Full outer join: cannot push filters on either side here without
        // changing the semantics of the query.
        return 0;
      case INNER:
        // Equijoin: can push on both sides and the semantics are preserved.
        canPushLeft = true;
        canPushRight = true;
        break;
      case LEFT:
        // LEFT JOIN: can only push to the left.
        canPushLeft = true;
        break;
      case RIGHT:
        // RIGHT JOIN: can only push to the right.
        canPushRight = true;
        break;
      case ANTI:
      case SEMI:
        // Semi and anti-semi join only return rows from the left input.
        // A consequent filter cannot possible refer to columns from the right input.
        canPushLeft = true;
        break;
    }

    // Determine if conditions refers to columns from both/single inputs.
    boolean refersLeft = false;
    boolean refersRight = false;
    int leftCols = join.getLeft().getRowType().getFieldCount();
    int rightCols = join.getRight().getRowType().getFieldCount();
    for (RexInputRef col : condition.accept(new RexInputRefCollector())) {
      if (col.getIndex() < leftCols) {
        refersLeft = true;
      } else {
        assert col.getIndex() < leftCols + rightCols;
        refersRight = true;
      }
    }

    if (refersLeft && !refersRight && canPushLeft) {
      return -1;
    } else if (!refersLeft && refersRight && canPushRight) {
      return 1;
    }
    return 0;
  }

  // Configuration for this rule.
  public interface Config extends RelRule.Config {
    Config DEFAULT =
        RelRule.Config.EMPTY
            .withOperandSupplier(
                builder ->
                    builder
                        .operand(LogicalFilter.class)
                        .oneInput(
                            nestedBuilder -> nestedBuilder.operand(LogicalJoin.class).anyInputs()))
            .as(Config.class);

    @Override
    default FilterPastJoin toRule() {
      return new FilterPastJoin(DEFAULT);
    }
  }
}
