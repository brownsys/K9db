package com.brownsys.k9db.schema;

import com.brownsys.k9db.nativelib.DataFlowGraphLibrary;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.sql.type.SqlTypeName;
import org.bytedeco.javacpp.BytePointer;

public class K9dbSchemaFactory {
  private DataFlowGraphLibrary.DataFlowGraphGenerator generator;

  public K9dbSchemaFactory(DataFlowGraphLibrary.DataFlowGraphGenerator generator) {
    this.generator = generator;
  }

  public CalciteSchema createSchema() {
    CalciteSchema schema = CalciteSchema.createRootSchema(false, false);
    DataFlowGraphLibrary.StringVector tables = generator.GetTables();
    for (int i = 0; i < tables.size(); i++) {
      BytePointer ccTableName = tables.get(i);
      K9dbTable table = this.createTable(ccTableName);
      schema.add(table.getTableName(), table);
    }
    return schema;
  }

  private K9dbTable createTable(BytePointer ccTableName) {
    K9dbTable table = new K9dbTable(ccTableName.getString());
    for (int j = 0; j < generator.GetTableColumnCount(ccTableName); j++) {
      String colName = generator.GetTableColumnName(ccTableName, j).getString();
      SqlTypeName type = this.enumToCalciteType(generator.GetTableColumnType(ccTableName, j));
      table.AddColumn(colName, type);
    }
    return table;
  }

  private SqlTypeName enumToCalciteType(int type) {
    switch (type) {
      case DataFlowGraphLibrary.INT:
      case DataFlowGraphLibrary.UINT:
        return SqlTypeName.INTEGER;
      case DataFlowGraphLibrary.TEXT:
        return SqlTypeName.VARCHAR;
      case DataFlowGraphLibrary.DATETIME:
        return SqlTypeName.TIMESTAMP;
      default:
        throw new IllegalArgumentException("Unsupported column type");
    }
  }
}
