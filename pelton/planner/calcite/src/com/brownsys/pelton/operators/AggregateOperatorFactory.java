package com.brownsys.pelton.operators;

import com.brownsys.pelton.PlanningContext;
import com.brownsys.pelton.nativelib.DataFlowGraphLibrary;
import java.util.ArrayList;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;

public class AggregateOperatorFactory {
  private final PlanningContext context;

  public AggregateOperatorFactory(PlanningContext context) {
    this.context = context;
  }

  public int createOperator(LogicalAggregate aggregate, ArrayList<Integer> children) {
    assert children.size() == 1;

    int[] groupCols = aggregate.getGroupSet().toArray();
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
        aggCol = aggCall.getArgList().get(0);
        break;
      default:
        throw new IllegalArgumentException("Invalid aggregate function");
    }

    return this.context
        .getGenerator()
        .AddAggregateOperator(children.get(0), groupCols, functionEnum, aggCol);
  }
}
