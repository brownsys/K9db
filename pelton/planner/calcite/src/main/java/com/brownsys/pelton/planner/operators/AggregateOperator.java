package com.brownsys.pelton.planner.operators;

import java.util.List;

import com.brownsys.pelton.planner.graph.Operator;

public class AggregateOperator extends Operator {
  private List<Integer> groupCols;
  private List<String> aggFuncs;
  private List<Integer> aggCols;

  public AggregateOperator() {
    this.name = "Aggregate";
  }

  public AggregateOperator(List<Integer> groupCols, List<String> aggFuncs, List<Integer> aggCols) {
    this.name = "Aggregate";
    this.groupCols = groupCols;
    this.aggFuncs = aggFuncs;
    this.aggCols = aggCols;
  }

  public List<Integer> getGroupCols() {
    return groupCols;
  }

  public void setGroupCols(List<Integer> groupCols) {
    this.groupCols = groupCols;
  }

  public List<String> getAggFuncs() {
    return aggFuncs;
  }

  public void setAggFuncs(List<String> aggFuncs) {
    this.aggFuncs = aggFuncs;
  }

  public List<Integer> getAggCols() {
    return aggCols;
  }

  public void setAggCols(List<Integer> aggCols) {
    this.aggCols = aggCols;
  }

}
