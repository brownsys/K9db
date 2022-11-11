#include "pelton/sql/rocksdb/project.h"

#include <memory>
#include <utility>

#include "glog/logging.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace sql {

Projection ProjectionSchema(
    const dataflow::SchemaRef &table_schema,
    const std::vector<sqlast::Select::ResultColumn> &columns) {
  // Special case: SELECT * can be handled quickly.
  if (columns.size() == 1) {
    if (columns.front().IsColumn() && columns.front().GetColumn() == "*") {
      return {table_schema, {}};
    }
  }

  CHECK_EQ(table_schema.keys().size(), 1u) << "Illegal keys " << table_schema;
  std::vector<std::string> names;
  std::vector<sqlast::ColumnDefinition::Type> types;
  std::vector<uint32_t> keys;
  std::vector<std::variant<uint32_t, sqlast::Value>> projections;
  for (size_t i = 0; i < columns.size(); i++) {
    const sqlast::Select::ResultColumn &result_column = columns.at(i);
    names.push_back(result_column.ToString());
    if (result_column.IsColumn()) {
      uint32_t idx = table_schema.IndexOf(result_column.GetColumn());
      types.push_back(table_schema.TypeOf(idx));
      projections.emplace_back(idx);
      if (idx == table_schema.keys().at(0)) {
        keys.push_back(i);
      }
    } else {
      const sqlast::Value &literal = result_column.GetLiteral();
      types.push_back(literal.ConvertType());
      projections.emplace_back(literal);
    }
  }
  return {dataflow::SchemaFactory::Create(names, types, keys),
          std::move(projections)};
}

dataflow::Record Project(const Projection &project,
                         const dataflow::Record &record) {
  dataflow::Record output{project.schema, false};
  for (size_t i = 0; i < project.schema.size(); i++) {
    const std::variant<uint32_t, sqlast::Value> &v = project.projections.at(i);
    if (v.index() == 0) {
      // Project column.
      uint32_t idx = std::get<uint32_t>(v);
      output.SetValue(record.GetValue(idx), i);
    } else {
      // Project literal.
      const sqlast::Value &l = std::get<sqlast::Value>(v);
      switch (l.type()) {
        case sqlast::Value::INT: {
          output.SetInt(l.GetInt(), i);
          break;
        }
        case sqlast::Value::TEXT: {
          output.SetString(std::make_unique<std::string>(l.GetString()), i);
          break;
        }
        default:
          LOG(FATAL) << "Unsupported literal projection type";
      }
    }
  }
  return output;
}

}  // namespace sql
}  // namespace pelton
