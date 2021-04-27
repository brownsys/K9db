package com.brownsys.pelton.operators;

import com.brownsys.pelton.PlanningContext;
import org.apache.calcite.rel.core.TableScan;

public class InputOperatorFactory {
  private final PlanningContext context;

  public InputOperatorFactory(PlanningContext context) {
    this.context = context;
  }

  public int createOperator(TableScan scan) {
    String tableName = scan.getTable().getQualifiedName().get(0);
    if (this.context.hasInputOperatorFor(tableName)) {
      return this.context.getInputOperatorFor(tableName);
    } else {
      int inputOperator = this.context.getGenerator().AddInputOperator(tableName);
      this.context.addInputOperator(tableName, inputOperator);
      return inputOperator;
    }
  }
}
