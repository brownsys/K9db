package com.brownsys.pelton.planner.operators;

import com.brownsys.pelton.planner.graph.Operator;
import java.util.ArrayList;

public class UnionOperator extends Operator {
  public UnionOperator() {
    this.name = "Union";
  }

  public UnionOperator(ArrayList<String> outSchema) {
    this.name = "Union";
    this.outSchema = outSchema;
  }
}
