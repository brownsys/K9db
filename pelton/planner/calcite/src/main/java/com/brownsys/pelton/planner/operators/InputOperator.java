package com.brownsys.pelton.planner.operators;

import java.util.ArrayList;

import com.brownsys.pelton.planner.graph.Operator;

public class InputOperator extends Operator {
  private String tableName;

  public InputOperator() {
    this.name = "Input";
  }

  public InputOperator(String tableName, ArrayList<String> outSchema) {
    this.name = "Input";
    this.tableName = tableName;
    this.outSchema = outSchema;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }
}
