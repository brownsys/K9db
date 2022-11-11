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
  for (const auto &[i, vals] : value_mapper.Values()) {
    sqlast::Value value = record.GetValue(i);
    if (value.type() == sqlast::Value::Type::UINT) {
      value = sqlast::Value(static_cast<int64_t>(value.GetUInt()));
    }
    if (std::find(vals.begin(), vals.end(), value) == vals.end()) {
      return false;
    }
  }
  return true;
}

}  // namespace sql
}  // namespace pelton
