package com.brownsys.pelton.planner.operators;

import java.util.ArrayList;

import com.brownsys.pelton.planner.graph.Operator;

public class ProjectOperator extends Operator {
  private ArrayList<Integer> cids;

  public ProjectOperator() {
    this.name = "Project";
  }

  public ProjectOperator(ArrayList<Integer> cids, ArrayList<String> outSchema) {
    this.name = "Project";
    this.cids = cids;
    this.outSchema = outSchema;
  }

  public ArrayList<Integer> getCids() {
    return cids;
  }

  public void setCids(ArrayList<Integer> cids) {
    this.cids = cids;
  }
}
