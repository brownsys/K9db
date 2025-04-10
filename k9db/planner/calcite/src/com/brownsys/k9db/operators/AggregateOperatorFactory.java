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

    // We only support a single or no aggregate function per operator at the moment.
    int functionEnum = DataFlowGraphLibrary.NO_AGGREGATE;
    int aggCol = 0;
    String aggName = "";
    if (aggregate.getAggCallList().size() > 0) {
      assert aggregate.getAggCallList().size() == 1;
      AggregateCall aggCall = aggregate.getAggCallList().get(0);
      switch (aggCall.getAggregation().getKind()) {
        case COUNT:
          functionEnum = DataFlowGraphLibrary.COUNT;
          assert aggCall.getArgList().size() <= 1;
          if (aggCall.getArgList().size() == 0) {
            aggCol = -1;
          } else {
            aggCol = this.context.getK9dbIndex(aggCall.getArgList().get(0));
          }
          break;
        case SUM:
          functionEnum = DataFlowGraphLibrary.SUM;
          assert aggCall.getArgList().size() == 1;
          aggCol = this.context.getK9dbIndex(aggCall.getArgList().get(0));
          break;
        case AVG:
          functionEnum = DataFlowGraphLibrary.AVG;
          assert aggCall.getArgList().size() == 1;
          aggCol = this.context.getK9dbIndex(aggCall.getArgList().get(0));
          break;
        default:
          throw new IllegalArgumentException("Invalid aggregate function");
      }

      this.context.identityTranslation(groupCols.length + 1, keyTranslation);

      // Get any alias assigned to the aggregate column.
      aggName = aggregate.getRowType().getFieldNames().get(groupCols.length);
      if (aggName.startsWith("EXPR$")) {
        aggName = "";
      }
    } else {
      this.context.identityTranslation(groupCols.length, keyTranslation);
    }

    return this.context
        .getGenerator()
        .AddAggregateOperator(children.get(0), groupCols, functionEnum, aggCol, aggName);
  }
}
