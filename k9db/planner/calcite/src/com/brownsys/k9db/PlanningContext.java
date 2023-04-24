package com.brownsys.k9db;

import com.brownsys.k9db.nativelib.DataFlowGraphLibrary;
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
  private static class SharedContext {
    public final DataFlowGraphLibrary.DataFlowGraphGenerator generator;
    public final Hashtable<String, Integer> inputOperators;

    public SharedContext(DataFlowGraphLibrary.DataFlowGraphGenerator generator) {
      this.generator = generator;
      this.inputOperators = new Hashtable<String, Integer>();
    }
  }

  // SharedContext is a singleton.
  private static SharedContext SINGLETON = null;

  public static void useGenerator(DataFlowGraphLibrary.DataFlowGraphGenerator generator) {
    PlanningContext.SINGLETON = new SharedContext(generator);
  }

  // Calcite's view of the schema within an operator and K9db's view of that
  // same operator is not the same: particularly, when a join on columns x == y
  // occurs, since we know that their values are identical, k9db only keeps
  // column x, and disregards column y. Calcite on the other hand keeps both.
  // This map is responsible for mapping a Calcite column index to the corresponding
  // column's index in k9db. Note: y will be mapped to x.
  private final HashMap<Integer, Integer> columnTranslation;
  private final ArrayList<Integer> keyColumns;
  private final SharedContext shared;
  private HashSet<Integer> orderColumns; // for lookups with > ?

  // The initial context we start from!
  public PlanningContext() {
    this.shared = PlanningContext.SINGLETON;
    this.keyColumns = new ArrayList<Integer>();
    this.columnTranslation = new HashMap<Integer, Integer>();
    this.orderColumns = new HashSet<Integer>();
  }

  // Called when we have sibling operators each making modification to their own contexts.
  // Currently, this can only happen if there is a union or join operator.
  public void merge(MergeOperation op, PlanningContext other) {
    switch (op) {
      case UNION:
        assert this.keyColumns.equals(other.keyColumns);
        assert this.columnTranslation.equals(other.columnTranslation);
        assert this.orderColumns.equals(other.orderColumns);
        break;
      case JOIN:
        assert this.orderColumns.isEmpty();
        assert other.orderColumns.isEmpty();
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

  // Get the shared k9db flow generator native API wrapper.
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

  public void addOrderColumn(int col) {
    this.orderColumns.add(col);
  }

  public HashSet<Integer> getOrderColumns() {
    return new HashSet<Integer>(this.orderColumns);
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
  public int getK9dbColumnCount() {
    HashSet<Integer> k9dbIndices = new HashSet<Integer>(this.columnTranslation.values());
    return k9dbIndices.size();
  }

  public int getK9dbIndex(int calciteIndex) {
    return this.columnTranslation.get(calciteIndex);
  }

  public void setDuplicate(int duplicateK9dbIndex, int actualK9dbIndex) {
    HashSet<Integer> keys = new HashSet<Integer>(this.columnTranslation.keySet());
    for (int key : keys) {
      int k9dbIndex = this.columnTranslation.get(key);
      if (k9dbIndex == duplicateK9dbIndex) {
        this.columnTranslation.put(key, actualK9dbIndex);
      } else if (k9dbIndex > duplicateK9dbIndex) {
        this.columnTranslation.put(key, k9dbIndex - 1);
      }
    }

    for (int i = 0; i < this.keyColumns.size(); i++) {
      int key = this.keyColumns.get(i);
      if (key == duplicateK9dbIndex) {
        if (!this.keyColumns.contains(actualK9dbIndex)) {
          this.keyColumns.set(i, actualK9dbIndex);
        } else {
          this.keyColumns.remove(i);
          i--;
        }
      } else if (key > duplicateK9dbIndex) {
        this.keyColumns.set(i, key - 1);
      }
    }

    HashSet<Integer> modOrderColumns = new HashSet<Integer>();
    for (Integer col : this.orderColumns) {
      if (col == duplicateK9dbIndex) {
        if (!this.orderColumns.contains(actualK9dbIndex)) {
          modOrderColumns.add(actualK9dbIndex);
        }
        continue;
      } else if (col > duplicateK9dbIndex) {
        modOrderColumns.add(col - 1);
      }
    }
    this.orderColumns = modOrderColumns;
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
        throw new IllegalArgumentException("Key column is dropped by projection");
      }
    }

    HashSet<Integer> modOrderColumns = new HashSet<Integer>();
    for (Integer col : this.orderColumns) {
      if (translation.containsKey(col)) {
        modOrderColumns.add(translation.get(col));
      } else {
        throw new IllegalArgumentException("Compare column is dropped by projection");
      }
    }
    this.orderColumns = modOrderColumns;
  }

  public void identityTranslation(int count, HashMap<Integer, Integer> keyTranslation) {
    if (!this.orderColumns.isEmpty()) {
      throw new IllegalArgumentException("Aggregate after filter with > ?; Use HAVING");
    }

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
