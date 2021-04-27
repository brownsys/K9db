package com.brownsys.pelton;

import com.brownsys.pelton.nativelib.DataFlowGraphLibrary;
import java.util.ArrayList;
import java.util.Hashtable;

public class PlanningContext {
  private final DataFlowGraphLibrary.DataFlowGraphGenerator generator;
  private final ArrayList<Integer> keyColumns;
  private final Hashtable<String, Integer> inputOperators;

  // NOTE(Ishan): Lobsters queries are very neat, i.e. they never "select *"
  // after a join, they always disambiguate the columns in projection.
  // Nevertheless, if things break we can fall back to avoiding deduplication
  // in join.
  public final ArrayList<ArrayList<Integer>> joinDuplicateColumns;

  public PlanningContext(DataFlowGraphLibrary.DataFlowGraphGenerator generator) {
    this.generator = generator;
    this.keyColumns = new ArrayList<Integer>();
    this.inputOperators = new Hashtable<String, Integer>();
    this.joinDuplicateColumns = new ArrayList<ArrayList<Integer>>();
  }

  public DataFlowGraphLibrary.DataFlowGraphGenerator getGenerator() {
    return generator;
  }

  // Materialized view keys.
  public void addMatViewKey(int col) {
    if (!this.keyColumns.contains(col)) {
      this.keyColumns.add(col);
    }
  }

  public int[] getMatViewKeys() {
    int[] result = new int[this.keyColumns.size()];
    for (int i = 0; i < this.keyColumns.size(); i++) {
      result[i] = this.keyColumns.get(i);
    }
    return result;
  }

  // Input Operators mapping.
  public boolean hasInputOperatorFor(String tablename) {
    return this.inputOperators.containsKey(tablename);
  }

  public void addInputOperator(String tablename, int operator) {
    this.inputOperators.put(tablename, operator);
  }

  public int getInputOperatorFor(String tablename) {
    return this.inputOperators.get(tablename);
  }
}
