package com.brownsys.k9db.operators;

import com.brownsys.k9db.PlanningContext;
import java.util.ArrayList;
import org.apache.calcite.rel.logical.LogicalUnion;

public class UnionOperatorFactory {
  private final PlanningContext context;

  public UnionOperatorFactory(PlanningContext context) {
    this.context = context;
  }

  public int createOperator(LogicalUnion union, ArrayList<Integer> children) {
    int[] ids = new int[children.size()];
    for (int i = 0; i < children.size(); i++) {
      ids[i] = children.get(i);
    }

    // Add a union operator.
    return this.context.getGenerator().AddUnionOperator(ids);
  }
}
