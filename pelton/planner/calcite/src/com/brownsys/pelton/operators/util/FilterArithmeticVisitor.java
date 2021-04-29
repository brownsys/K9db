package com.brownsys.pelton.operators.util;

import com.brownsys.pelton.operators.FilterOperatorFactory;
import java.util.LinkedList;
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
import org.apache.calcite.sql.SqlKind;

// Checks whether a given filter condition contains arithmetic expressions.
// This visitor can return either:
// 1. true: when the condition contains arithmetic (+ or -).
// 2. false: if the condition does not contain any arithmetic.
// 3. throws IllegalArgumentException: if the condition has some unsupported shape
//    or constructs. Importantly, an OR inside an AND.
public class FilterArithmeticVisitor implements RexVisitor<Boolean> {
  private boolean insideAnd;
  private LinkedList<RexCall> arithmeticNodes;

  public FilterArithmeticVisitor() {
    this.insideAnd = false;
    this.arithmeticNodes = new LinkedList<RexCall>();
  }

  public List<RexCall> getArithmeticNodes() {
    return this.arithmeticNodes;
  }

  @Override
  public Boolean visitCall(RexCall arg0) {
    boolean tmp = this.insideAnd;
    boolean hasArithmetic = false;
    if (arg0.isA(SqlKind.AND)) {
      this.insideAnd = true;
    } else if (arg0.isA(SqlKind.OR)) {
      if (this.insideAnd) {
        throw new IllegalArgumentException("Cannot have an OR inside an AND in filter!");
      }
    } else if (arg0.isA(SqlKind.PLUS) || arg0.isA(SqlKind.MINUS)) {
      hasArithmetic = true;
      this.arithmeticNodes.addFirst(arg0);
    } else if (!arg0.isA(FilterOperatorFactory.FILTER_OPERATIONS)) {
      throw new IllegalArgumentException("Unsupported expression in filter!");
    }

    for (RexNode operand : arg0.getOperands()) {
      Boolean b = operand.accept(this);
      hasArithmetic = hasArithmetic || b;
    }

    this.insideAnd = tmp;
    return hasArithmetic;
  }

  @Override
  public Boolean visitDynamicParam(RexDynamicParam arg0) {
    return false;
  }

  @Override
  public Boolean visitInputRef(RexInputRef arg0) {
    return false;
  }

  @Override
  public Boolean visitLiteral(RexLiteral arg0) {
    return false;
  }

  // Unsupported constructs.
  @Override
  public Boolean visitCorrelVariable(RexCorrelVariable arg0) {
    throw new IllegalArgumentException(
        "Unsupported RexNode in filter node " + arg0.getClass().getName());
  }

  @Override
  public Boolean visitFieldAccess(RexFieldAccess arg0) {
    throw new IllegalArgumentException(
        "Unsupported RexNode in filter node " + arg0.getClass().getName());
  }

  @Override
  public Boolean visitLocalRef(RexLocalRef arg0) {
    throw new IllegalArgumentException(
        "Unsupported RexNode in filter node " + arg0.getClass().getName());
  }

  @Override
  public Boolean visitOver(RexOver arg0) {
    throw new IllegalArgumentException(
        "Unsupported RexNode in filter node " + arg0.getClass().getName());
  }

  @Override
  public Boolean visitPatternFieldRef(RexPatternFieldRef arg0) {
    throw new IllegalArgumentException(
        "Unsupported RexNode in filter node " + arg0.getClass().getName());
  }

  @Override
  public Boolean visitRangeRef(RexRangeRef arg0) {
    throw new IllegalArgumentException(
        "Unsupported RexNode in filter node " + arg0.getClass().getName());
  }

  @Override
  public Boolean visitSubQuery(RexSubQuery arg0) {
    throw new IllegalArgumentException(
        "Unsupported RexNode in filter node " + arg0.getClass().getName());
  }

  @Override
  public Boolean visitTableInputRef(RexTableInputRef arg0) {
    throw new IllegalArgumentException(
        "Unsupported RexNode in filter node " + arg0.getClass().getName());
  }
}