package com.brownsys.pelton;

import com.brownsys.pelton.nativelib.DataFlowGraphLibrary;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;

public class PlanningContext {
  public enum MergeOperation {
    JOIN,
    UNION,
    OTHER;
  }

  // This context object is shared between all sibling planning contexts.
  private class SharedContext {
    public final DataFlowGraphLibrary.DataFlowGraphGenerator generator;
    public final Hashtable<String, Integer> inputOperators;

    public SharedContext(DataFlowGraphLibrary.DataFlowGraphGenerator generator) {
      this.generator = generator;
      this.inputOperators = new Hashtable<String, Integer>();
    }
  }

  // Calcite's view of the schema within an operator and Pelton's view of that
  // same operator is not the same: particularly, when a join on columns x == y
  // occurs, since we know that their values are identical, pelton only keeps
  // column x, and disregards column y. Calcite on the other hand keeps both.
  // This map is responsible for mapping a Calcite column index to the corresponding
  // column's index in pelton. Note: y will be mapped to x.
  private final HashMap<Integer, Integer> columnTranslation;
  private final ArrayList<Integer> keyColumns;
  private final SharedContext shared;

  // The initial context we start from!
  public PlanningContext(DataFlowGraphLibrary.DataFlowGraphGenerator generator) {
    this.shared = new SharedContext(generator);
    this.keyColumns = new ArrayList<Integer>();
    this.columnTranslation = new HashMap<Integer, Integer>();
  }

  // Called when we have sibling operators each making modification to their own contexts.
  // Currently, this can only happen if there is a union or join operator.
  public void merge(MergeOperation op, PlanningContext other) {
    switch (op) {
      case UNION:
        assert this.keyColumns.equals(other.keyColumns);
        assert this.columnTranslation.equals(other.columnTranslation);
        break;
      case JOIN:
        HashSet<Integer> ids = new HashSet<Integer>();
        for (Map.Entry<Integer, Integer> entry : this.columnTranslation.entrySet()) {
          ids.add(entry.getValue());
        }
        int leftCount = this.columnTranslation.size();
        int realCount = ids.size();
        for (Map.Entry<Integer, Integer> entry : other.columnTranslation.entrySet()) {
          this.columnTranslation.put(entry.getKey() + leftCount, entry.getValue() + realCount);
        }
        for (int key : other.keyColumns) {
          this.keyColumns.add(realCount + key);
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown merge operation " + op);
    }
  }

  // Get the shared pelton flow generator native API wrapper.
  public DataFlowGraphLibrary.DataFlowGraphGenerator getGenerator() {
    return this.shared.generator;
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
    return this.shared.inputOperators.containsKey(tablename);
  }

  public void addInputOperator(String tablename, int operator) {
    this.shared.inputOperators.put(tablename, operator);
  }

  public int getInputOperatorFor(String tablename) {
    return this.shared.inputOperators.get(tablename);
  }

  // Duplicated columns (because of a join).
  public int getPeltonIndex(int calciteIndex) {
    return this.columnTranslation.get(calciteIndex);
  }

  public void setDuplicate(int duplicatePeltonIndex, int actualPeltonIndex) {
    HashSet<Integer> keys = new HashSet<Integer>(this.columnTranslation.keySet());
    for (int key : keys) {
      int peltonIndex = this.columnTranslation.get(key);
      if (peltonIndex == duplicatePeltonIndex) {
        this.columnTranslation.put(key, actualPeltonIndex);
      } else if (peltonIndex > duplicatePeltonIndex) {
        this.columnTranslation.put(key, peltonIndex - 1);
      }
    }

    for (int i = 0; i < this.keyColumns.size(); i++) {
      int key = this.keyColumns.get(i);
      if (key == duplicatePeltonIndex) {
        if (!this.keyColumns.contains(actualPeltonIndex)) {
          this.keyColumns.set(i, actualPeltonIndex);
        } else {
          this.keyColumns.remove(i);
          i--;
        }
      } else if (key > duplicatePeltonIndex) {
        this.keyColumns.set(i, key - 1);
      }
    }
  }

  public void setColumnTranslation(
      HashMap<Integer, Integer> translation, HashMap<Integer, Integer> keyTranslation) {
    this.columnTranslation.clear();
    this.columnTranslation.putAll(translation);

    for (int i = 0; i < this.keyColumns.size(); i++) {
      int key = this.keyColumns.get(i);
      if (keyTranslation.containsKey(key)) {
        this.keyColumns.set(i, keyTranslation.get(key));
      } else {
        this.keyColumns.remove(i);
        i--;
      }
    }
  }

  public void identityTranslation(int count, HashMap<Integer, Integer> keyTranslation) {
    this.columnTranslation.clear();
    for (int i = 0; i < count; i++) {
      this.columnTranslation.put(i, i);
    }
    for (int i = 0; i < this.keyColumns.size(); i++) {
      int key = this.keyColumns.get(i);
      if (keyTranslation.containsKey(key)) {
        this.keyColumns.set(i, keyTranslation.get(key));
      } else {
        this.keyColumns.remove(i);
        i--;
      }
    }
  }
}
