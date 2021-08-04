package com.brownsys.pelton.schema;

import com.brownsys.pelton.nativelib.DataFlowGraphLibrary;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.sql.type.SqlTypeName;
import org.bytedeco.javacpp.BytePointer;

public class PeltonSchemaFactory {
  private DataFlowGraphLibrary.DataFlowGraphGenerator generator;

  public PeltonSchemaFactory(DataFlowGraphLibrary.DataFlowGraphGenerator generator) {
    this.generator = generator;
  }

  public CalciteSchema createSchema() {
    CalciteSchema schema = CalciteSchema.createRootSchema(false, false);
    DataFlowGraphLibrary.StringVector tables = generator.GetTables();
    for (int i = 0; i < tables.size(); i++) {
      BytePointer ccTableName = tables.get(i);
      PeltonTable table = this.createTable(ccTableName);
      schema.add(table.getTableName(), table);
    }
    return schema;
  }

  private PeltonTable createTable(BytePointer ccTableName) {
    PeltonTable table = new PeltonTable(ccTableName.getString());
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
