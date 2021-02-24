package com.brownsys.pelton.planner.operators;

import java.util.ArrayList;

import com.brownsys.pelton.planner.graph.Operator;

public class UnionOperator extends Operator {
  public UnionOperator() {
    this.name = "Union";
  }

  public UnionOperator(ArrayList<String> outSchema) {
    this.name = "Union";
    this.outSchema = outSchema;
  }
}
