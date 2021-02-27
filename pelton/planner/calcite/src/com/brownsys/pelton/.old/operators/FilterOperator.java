package com.brownsys.pelton.planner.operators;

import com.brownsys.pelton.planner.graph.Operator;
import java.util.ArrayList;

public class FilterOperator extends Operator {
  ArrayList<Integer> cids;
  ArrayList<String> ops;
  ArrayList<String> vals;
  ArrayList<String> valTypes;

  public FilterOperator() {
    this.name = "Filter";
  }

  public FilterOperator(
      Integer id,
      ArrayList<Integer> cids,
      ArrayList<String> ops,
      ArrayList<String> vals,
      ArrayList<String> valTypes,
      ArrayList<String> outSchema) {
    this.id = id;
    this.name = "Filter";
    this.cids = cids;
    this.ops = ops;
    this.vals = vals;
    this.valTypes = valTypes;
    this.outSchema = outSchema;
  }

  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public ArrayList<String> getValTypes() {
    return valTypes;
  }

  public void setValTypes(ArrayList<String> valTypes) {
    this.valTypes = valTypes;
  }

  public ArrayList<Integer> getCids() {
    return cids;
  }

  public void setCids(ArrayList<Integer> cids) {
    this.cids = cids;
  }

  public ArrayList<String> getOps() {
    return ops;
  }

  public void setOps(ArrayList<String> ops) {
    this.ops = ops;
  }

  public ArrayList<String> getVals() {
    return vals;
  }

  public void setVals(ArrayList<String> vals) {
    this.vals = vals;
  }
}
