#include "pelton/sql/rocksdb/filter.h"

#include <algorithm>
#include <string>
#include <vector>

#include "glog/logging.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace sql {

bool InMemoryFilter(const sqlast::ValueMapper &value_mapper,
                    const dataflow::Record &record) {
  for (size_t i = 0; i < record.schema().size(); i++) {
    if (value_mapper.HasBefore(i)) {
      const std::vector<std::string> &vals = value_mapper.Before(i);
      std::string value;
      if (record.IsNull(i)) {
        value = "NULL";
      } else {
        switch (record.schema().TypeOf(i)) {
          case sqlast::ColumnDefinition::Type::UINT: {
            value = std::to_string(record.GetUInt(i));
            break;
          }
          case sqlast::ColumnDefinition::Type::INT: {
            value = std::to_string(record.GetInt(i));
            break;
          }
          case sqlast::ColumnDefinition::Type::TEXT: {
            value = '\'' + record.GetString(i) + '\'';
            break;
          }
          case sqlast::ColumnDefinition::Type::DATETIME: {
            value = '\'' + record.GetDateTime(i) + '\'';
            break;
          }
          default: {
            LOG(FATAL) << "UNREACHABLE";
          }
        }
      }

      if (std::find(vals.begin(), vals.end(), value) == vals.end()) {
        return false;
      }
    }
  }
  return true;
}

}  // namespace sql
}  // namespace pelton
