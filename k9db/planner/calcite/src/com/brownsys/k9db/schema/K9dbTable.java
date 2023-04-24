package com.brownsys.k9db.schema;

import java.util.ArrayList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

public class K9dbTable extends AbstractTable {
  private String tableName;
  private ArrayList<String> columnNames;
  private ArrayList<SqlTypeName> columnTypes;

  public K9dbTable(String tableName) {
    super();
    this.tableName = tableName;
    this.columnNames = new ArrayList<String>();
    this.columnTypes = new ArrayList<SqlTypeName>();
  }

  public String getTableName() {
    return tableName;
  }

  public void AddColumn(String columnName, SqlTypeName columnType) {
    this.columnNames.add(columnName);
    this.columnTypes.add(columnType);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory factory) {
    ArrayList<RelDataType> columnCreatedTypes = new ArrayList<RelDataType>();
    for (SqlTypeName typeName : this.columnTypes) {
      columnCreatedTypes.add(factory.createSqlType(typeName));
    }
    return factory.createStructType(columnCreatedTypes, this.columnNames);
  }
}
