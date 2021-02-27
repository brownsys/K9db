package com.brownsys.pelton.planner.schema;

import java.util.ArrayList;

public class TableSchema {
  private String tableName;
  private ArrayList<String> colNames;
  private ArrayList<String> colTypes;
  private ArrayList<Integer> colSizes;

  public TableSchema() {}

  public TableSchema(
      String tableName,
      ArrayList<String> colNames,
      ArrayList<String> colTypes,
      ArrayList<Integer> size) {
    this.tableName = tableName;
    this.colNames = colNames;
    this.colTypes = colTypes;
    this.colSizes = size;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public ArrayList<String> getColNames() {
    return colNames;
  }

  public void setColNames(ArrayList<String> colNames) {
    this.colNames = colNames;
  }

  public ArrayList<String> getColTypes() {
    return colTypes;
  }

  public void setColTypes(ArrayList<String> colTypes) {
    this.colTypes = colTypes;
  }

  public ArrayList<Integer> getColSizes() {
    return colSizes;
  }

  public void setColSizes(ArrayList<Integer> colSizes) {
    this.colSizes = colSizes;
  }
}
