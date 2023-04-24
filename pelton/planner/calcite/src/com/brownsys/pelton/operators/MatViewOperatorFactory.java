package com.brownsys.pelton.operators;

import com.brownsys.pelton.PlanningContext;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.calcite.rel.RelFieldCollation;
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
    HashSet<Integer> orderColumns = this.context.getOrderColumns();
    int[] orderCols = new int[orderColumns.size()];
    int i = 0;
    for (Integer col : orderColumns) {
      orderCols[i] = col;
      i++;
    }
    this.createOperator(children, orderCols, -1, 0);
  }

  public void createOperator(LogicalSort sort, ArrayList<Integer> children) {
    // Display a warning that DESCENDING is ignored.
    for (RelFieldCollation field : sort.getCollation().getFieldCollations()) {
      if (field.getDirection().isDescending()) {
        System.out.println("Warning: DESCENDING order ignored");
      }
    }

    // Find the (explicit) sorting parameters.
    List<RexNode> exps = sort.getSortExps();
    int[] sortingCols = new int[exps.size()];
    for (int i = 0; i < exps.size(); i++) {
      RexNode exp = exps.get(i);
      assert exp instanceof RexInputRef;
      int calciteIndex = ((RexInputRef) exp).getIndex();
      sortingCols[i] = this.context.getPeltonIndex(calciteIndex);
    }

    // Get (implicit) sorting paramteres (from col > ? conditions).
    HashSet<Integer> contextSortingCols = this.context.getOrderColumns();
    if (!contextSortingCols.isEmpty()) {
      for (int i : sortingCols) {
        if (!contextSortingCols.contains(i)) {
          throw new IllegalArgumentException("Mismatched ORDER BY and > ?");
        }
      }
      sortingCols = new int[contextSortingCols.size()];
      int i = 0;
      for (Integer col : contextSortingCols) {
        sortingCols[i] = col;
        i++;
      }
    }

    // If offset or fetch (e.g. limit) is null, it means the query does not contain a LIMIT (, )
    // expression. If they are not null, they may also be "?", in which case we keep the default
    // unspecified -1 values for them.
    int limit = -1;
    int offset = 0;
    if (sort.offset != null && !(sort.offset instanceof RexDynamicParam)) {
      assert sort.offset instanceof RexLiteral;
      offset = RexLiteral.intValue(sort.offset);
    }
    if (sort.fetch != null && !(sort.fetch instanceof RexDynamicParam)) {
      assert sort.fetch instanceof RexLiteral;
      limit = RexLiteral.intValue(sort.fetch);
    }

    this.createOperator(children, sortingCols, limit, offset);
  }

  private void createOperator(
      ArrayList<Integer> children, int[] sortingCols, int limit, int offset) {
    assert children.size() == 1;
    int[] keyCols = this.context.getMatViewKeys();
    // We do not add this to children, a matview cannot be a parent for other operators.
    this.context
        .getGenerator()
        .AddMatviewOperator(children.get(0), keyCols, sortingCols, limit, offset);
  }
}
