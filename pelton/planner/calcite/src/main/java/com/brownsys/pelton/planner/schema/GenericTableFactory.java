package com.brownsys.pelton.planner.schema;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

import com.brownsys.pelton.planner.QueryPlanner;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;

public class GenericTableFactory implements TableFactory<Table> {
  private HashMap<String, TableSchema> tableSchemaMap = new HashMap<String, TableSchema>();

  @Override
  public Table create(SchemaPlus schema, String name, Map<String, Object> operand, RelDataType rowType) {
    // Dummy
    final Object[][] rows = {};
    if (tableSchemaMap.isEmpty()) {
      String tableSchemas = null;
      try {
        tableSchemas = Resources.toString(QueryPlanner.class.getResource("/table-schemas.json"),
            Charset.defaultCharset());
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      ObjectMapper objectMapper = new ObjectMapper();
      List<TableSchema> schemas = null;
      try {
        schemas = objectMapper.readValue(tableSchemas, new TypeReference<List<TableSchema>>() {
        });
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      for (TableSchema ts : schemas) {
        tableSchemaMap.put(ts.getTableName(), ts);
      }
    }

    return new GenericTable(ImmutableList.copyOf(rows), tableSchemaMap.get(name));
  }

  public static class GenericTable implements ScannableTable {
    protected RelProtoDataType protoRowType;
    private final ImmutableList<Object[]> rows;

    public GenericTable(ImmutableList<Object[]> rows, final TableSchema tableSchema) {
      this.protoRowType = new RelProtoDataType() {
        // deprecated use this instead: https://calcite.apache.org/javadocAggregate/org/apache/calcite/rel/type/RelDataTypeFactory.Builder.html
        public RelDataType apply(RelDataTypeFactory a) {
          FieldInfoBuilder builder = new RelDataTypeFactory.FieldInfoBuilder(a);
          for (int i = 0; i < tableSchema.getColNames().size(); i++) {
            if (tableSchema.getColSizes().get(i) > 0) {
              builder.add(tableSchema.getColNames().get(i), SqlTypeName.valueOf(tableSchema.getColTypes().get(i)),
                  tableSchema.getColSizes().get(i));
            } else {
              builder.add(tableSchema.getColNames().get(i), SqlTypeName.valueOf(tableSchema.getColTypes().get(i)));
            }
          }
          return builder.build();
        }
      };
      this.rows = rows;
    }

    public Enumerable<Object[]> scan(DataContext root) {
      return Linq4j.asEnumerable(rows);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return protoRowType.apply(typeFactory);
    }

    @Override
    public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }

    @Override
    public Schema.TableType getJdbcTableType() {
      return Schema.TableType.TABLE;
    }

    public boolean isRolledUp(String column) {
      // TODO Auto-generated method stub
      return false;
    }

    public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, SqlNode parent,
        CalciteConnectionConfig config) {
      // TODO Auto-generated method stub
      return false;
    }
  }
}
