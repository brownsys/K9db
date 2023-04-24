package com.brownsys.k9db.operators;

import com.brownsys.k9db.PlanningContext;
import java.util.HashMap;
import org.apache.calcite.rel.core.TableScan;

public class InputOperatorFactory {
  private final PlanningContext context;

  public InputOperatorFactory(PlanningContext context) {
    this.context = context;
  }

  public int createOperator(TableScan scan) {
    this.context.identityTranslation(
        scan.getRowType().getFieldCount(), new HashMap<Integer, Integer>());

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
