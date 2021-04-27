package com.brownsys.pelton.operators.util;

import com.brownsys.pelton.operators.FilterOperatorFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;

// Checks that the shape of the filter condition is good!
public class FilterShapeVisitor extends RexVisitorImpl<Boolean> {
  private boolean insideAnd = false;

  public FilterShapeVisitor() {
    super(false);
    this.insideAnd = false;
  }

  @Override
  public Boolean visitCall(RexCall arg0) {
    boolean tmp = this.insideAnd;
    if (arg0.isA(SqlKind.AND)) {
      this.insideAnd = true;
    } else if (arg0.isA(SqlKind.OR)) {
      if (this.insideAnd) {
        throw new IllegalArgumentException("Cannot have an OR inside an AND in filter!");
      }
    } else if (!arg0.isA(FilterOperatorFactory.FILTER_OPERATIONS)) {
      throw new IllegalArgumentException("Unsupported expression in filter!");
    }

    for (RexNode operand : arg0.getOperands()) {
      Boolean b = operand.accept(this);
      if (b == null || !b) {
        return false;
      }
    }

    this.insideAnd = tmp;
    return true;
  }

  @Override
  public Boolean visitDynamicParam(RexDynamicParam arg0) {
    return true;
  }

  @Override
  public Boolean visitInputRef(RexInputRef arg0) {
    return true;
  }

  @Override
  public Boolean visitLiteral(RexLiteral arg0) {
    return true;
  }
}
