package com.brownsys.k9db.util;

import java.util.HashSet;
import java.util.List;
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

// Returns a set that contains all columns referred to by the analyzed
// RexNode tree (e.g. filter condition).
public class RexInputRefCollector implements RexVisitor<HashSet<RexInputRef>> {

  @Override
  public HashSet<RexInputRef> visitInputRef(RexInputRef arg0) {
    return new HashSet<RexInputRef>(List.of(arg0));
  }

  @Override
  public HashSet<RexInputRef> visitLocalRef(RexLocalRef arg0) {
    throw new IllegalArgumentException(
        "Unsupported RexNode in filter node " + arg0.getClass().getName());
  }

  @Override
  public HashSet<RexInputRef> visitLiteral(RexLiteral arg0) {
    return new HashSet<RexInputRef>();
  }

  @Override
  public HashSet<RexInputRef> visitCall(RexCall arg0) {
    HashSet<RexInputRef> result = new HashSet<RexInputRef>();
    for (RexNode operand : arg0.getOperands()) {
      result.addAll(operand.accept(this));
    }
    return result;
  }

  @Override
  public HashSet<RexInputRef> visitOver(RexOver arg0) {
    throw new IllegalArgumentException(
        "Unsupported RexNode in filter node " + arg0.getClass().getName());
  }

  @Override
  public HashSet<RexInputRef> visitCorrelVariable(RexCorrelVariable arg0) {
    throw new IllegalArgumentException(
        "Unsupported RexNode in filter node " + arg0.getClass().getName());
  }

  @Override
  public HashSet<RexInputRef> visitDynamicParam(RexDynamicParam arg0) {
    return new HashSet<RexInputRef>();
  }

  @Override
  public HashSet<RexInputRef> visitRangeRef(RexRangeRef arg0) {
    throw new IllegalArgumentException(
        "Unsupported RexNode in filter node " + arg0.getClass().getName());
  }

  @Override
  public HashSet<RexInputRef> visitFieldAccess(RexFieldAccess arg0) {
    throw new IllegalArgumentException(
        "Unsupported RexNode in filter node " + arg0.getClass().getName());
  }

  @Override
  public HashSet<RexInputRef> visitSubQuery(RexSubQuery arg0) {
    throw new IllegalArgumentException(
        "Unsupported RexNode in filter node " + arg0.getClass().getName());
  }

  @Override
  public HashSet<RexInputRef> visitTableInputRef(RexTableInputRef arg0) {
    throw new IllegalArgumentException(
        "Unsupported RexNode in filter node " + arg0.getClass().getName());
  }

  @Override
  public HashSet<RexInputRef> visitPatternFieldRef(RexPatternFieldRef arg0) {
    throw new IllegalArgumentException(
        "Unsupported RexNode in filter node " + arg0.getClass().getName());
  }
}
