package com.brownsys.k9db.operators;

import com.brownsys.k9db.PlanningContext;
import com.brownsys.k9db.nativelib.DataFlowGraphLibrary;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;

public class AggregateOperatorFactory {
  private final PlanningContext context;

  public AggregateOperatorFactory(PlanningContext context) {
    this.context = context;
  }

  public int createOperator(LogicalAggregate aggregate, ArrayList<Integer> children) {
    assert children.size() == 1;

    HashMap<Integer, Integer> keyTranslation = new HashMap<Integer, Integer>();
    int[] groupCols = aggregate.getGroupSet().toArray();
    for (int i = 0; i < groupCols.length; i++) {
      groupCols[i] = this.context.getK9dbIndex(groupCols[i]);
      keyTranslation.put(groupCols[i], i);
    }

    // We only support a single aggregate function per operator at the moment
    assert aggregate.getAggCallList().size() == 1;
    AggregateCall aggCall = aggregate.getAggCallList().get(0);
    int functionEnum = -1;
    int aggCol = -1;
    switch (aggCall.getAggregation().getKind()) {
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
        aggCol = this.context.getK9dbIndex(aggCall.getArgList().get(0));
        break;
      default:
        throw new IllegalArgumentException("Invalid aggregate function");
    }

    this.context.identityTranslation(groupCols.length + 1, keyTranslation);

    // Get any alias assigned to the aggregate column.
    String aggName = aggregate.getRowType().getFieldNames().get(groupCols.length);
    if (aggName.startsWith("EXPR$")) {
      aggName = "";
    }

    return this.context
        .getGenerator()
        .AddAggregateOperator(children.get(0), groupCols, functionEnum, aggCol, aggName);
  }
}
