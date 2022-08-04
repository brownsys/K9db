#include "pelton/sql/rocksdb/project.h"

#include <memory>
#include <utility>

#include "glog/logging.h"

namespace pelton {
namespace sql {

#define IS_LITERAL(col_name) \
  (col_name.at(0) >= '0' && col_name.at(0) <= '9') || col_name.at(0) == '\''

Projection ProjectionSchema(const dataflow::SchemaRef &table_schema,
                            const std::vector<std::string> &columns) {
  CHECK_EQ(table_schema.keys().size(), 1u) << "Illegal keys " << table_schema;
  std::vector<std::string> names;
  std::vector<sqlast::ColumnDefinition::Type> types;
  std::vector<uint32_t> keys;
  std::vector<bool> literals;
  std::vector<std::variant<uint32_t, int64_t, std::string>> map;
  for (size_t i = 0; i < columns.size(); i++) {
    const std::string &col_name = columns.at(i);
    names.push_back(col_name);
    if (IS_LITERAL(col_name)) {
      literals.push_back(true);
      if (col_name.at(0) == '\'') {
        types.push_back(sqlast::ColumnDefinition::Type::TEXT);
        map.emplace_back(col_name.substr(1, col_name.size() - 2));
      } else {
        types.push_back(sqlast::ColumnDefinition::Type::INT);
        map.emplace_back(std::stoll(col_name));
      }
    } else {
      literals.push_back(false);
      uint32_t idx = table_schema.IndexOf(col_name);
      types.push_back(table_schema.TypeOf(idx));
      map.emplace_back(idx);
      if (idx == keys.at(0)) {
        keys.push_back(i);
      }
    }
  }
  return {dataflow::SchemaFactory::Create(names, types, keys),
          std::move(literals), std::move(map)};
}

dataflow::Record Project(const Projection &project,
                         const dataflow::Record &record) {
  dataflow::Record output{project.schema, false};
  for (size_t i = 0; i < project.schema.size(); i++) {
    if (project.literals.at(i)) {
      // Project literal.
      const auto &literal = project.map.at(i);
      if (literal.index() == 1) {
        output.SetInt(std::get<int64_t>(literal), i);
      } else if (literal.index() == 2) {
        const std::string &str = std::get<std::string>(literal);
        output.SetString(std::make_unique<std::string>(str), i);
      } else {
        LOG(FATAL) << "UNREACHABLE";
      }
    } else {
      // Project column.
      CHECK_EQ(project.map.at(i).index(), 0u);
      uint32_t idx = std::get<uint32_t>(project.map.at(i));
      if (record.IsNull(idx)) {
        output.SetNull(true, i);
        continue;
      }
      switch (project.schema.TypeOf(i)) {
        case sqlast::ColumnDefinition::Type::UINT: {
          output.SetUInt(record.GetUInt(idx), i);
          break;
        }
        case sqlast::ColumnDefinition::Type::INT: {
          output.SetInt(record.GetInt(idx), i);
          break;
        }
        case sqlast::ColumnDefinition::Type::TEXT: {
          const std::string &str = record.GetString(idx);
          output.SetString(std::make_unique<std::string>(str), i);
          break;
        }
        case sqlast::ColumnDefinition::Type::DATETIME: {
          const std::string &str = record.GetDateTime(idx);
          output.SetDateTime(std::make_unique<std::string>(str), i);
          break;
        }
      }
    }
  }

  return output;
}

}  // namespace sql
}  // namespace pelton
