package com.brownsys.pelton.operators;

import com.brownsys.pelton.PlanningContext;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

public class MatViewOperatorFactory {
  private final PlanningContext context;

  public MatViewOperatorFactory(PlanningContext context) {
    this.context = context;
  }

  public void createOperator(ArrayList<Integer> children) {
    // Add a materialized view linked to the last node in the graph.
    assert children.size() == 1;
    int[] keyCols = this.context.getMatViewKeys();
    this.context.getGenerator().AddMatviewOperator(children.get(0), keyCols);
  }

  public void createOperator(LogicalSort sort, ArrayList<Integer> children) {
    assert children.size() == 1;

    // Find the sorting parameters.
    int[] keyCols = this.context.getMatViewKeys();

    List<RexNode> exps = sort.getSortExps();
    int[] sortingCols = new int[exps.size()];
    int limit = -1;
    int offset = 0;
    for (int i = 0; i < exps.size(); i++) {
      RexNode exp = exps.get(i);
      assert exp instanceof RexInputRef;
      int calciteIndex = ((RexInputRef) exp).getIndex();
      sortingCols[i] = this.context.getPeltonIndex(calciteIndex);
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
    this.context
        .getGenerator()
        .AddMatviewOperator(children.get(0), keyCols, sortingCols, limit, offset);
  }
}
