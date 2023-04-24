package com.brownsys.k9db.util;

import java.util.ArrayList;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;

// Returns a copy of the given RexNode tree where every RexInputRef (i.e. column access)
// is changed by the given offset.
public class RexInputRefOffsetter implements RexVisitor<RexNode> {
  // The offset to apply to column indices.
  private final int offset;

  public RexInputRefOffsetter(int offset) {
    this.offset = offset;
  }

  @Override
  public RexNode visitInputRef(RexInputRef arg0) {
    return new RexInputRef(arg0.getIndex() + this.offset, arg0.getType());
  }

  @Override
  public RexNode visitLocalRef(RexLocalRef arg0) {
    throw new IllegalArgumentException(
        "Unsupported RexNode in filter node " + arg0.getClass().getName());
  }

  @Override
  public RexNode visitLiteral(RexLiteral arg0) {
    return arg0;
  }

  @Override
  public RexNode visitCall(RexCall call) {
    ArrayList<RexNode> operands = new ArrayList<RexNode>();
    for (RexNode operand : call.getOperands()) {
      operands.add(operand.accept(this));
    }
    return call.clone(call.getType(), operands);
  }

  @Override
  public RexNode visitOver(RexOver arg0) {
    throw new IllegalArgumentException(
        "Unsupported RexNode in filter node " + arg0.getClass().getName());
  }

  @Override
  public RexNode visitCorrelVariable(RexCorrelVariable arg0) {
    throw new IllegalArgumentException(
        "Unsupported RexNode in filter node " + arg0.getClass().getName());
  }

  @Override
  public RexNode visitDynamicParam(RexDynamicParam arg0) {
    return arg0;
  }

  @Override
  public RexNode visitRangeRef(RexRangeRef arg0) {
    throw new IllegalArgumentException(
        "Unsupported RexNode in filter node " + arg0.getClass().getName());
  }

  @Override
  public RexNode visitFieldAccess(RexFieldAccess arg0) {
    throw new IllegalArgumentException(
        "Unsupported RexNode in filter node " + arg0.getClass().getName());
  }

  @Override
  public RexNode visitSubQuery(RexSubQuery arg0) {
    throw new IllegalArgumentException(
        "Unsupported RexNode in filter node " + arg0.getClass().getName());
  }

  @Override
  public RexNode visitTableInputRef(RexTableInputRef arg0) {
    throw new IllegalArgumentException(
        "Unsupported RexNode in filter node " + arg0.getClass().getName());
  }

  @Override
  public RexNode visitPatternFieldRef(RexPatternFieldRef arg0) {
    throw new IllegalArgumentException(
        "Unsupported RexNode in filter node " + arg0.getClass().getName());
  }
}
