#include "pelton/dataflow/ops/filter.h"

#include <utility>
#include <vector>

#include "pelton/dataflow/record.h"
#include "pelton/dataflow/record_utils.h"

namespace pelton {
namespace dataflow {

bool FilterOperator::process(std::vector<Record>& rs,
                             std::vector<Record>& out_rs) {
  for (Record& r : rs) {
    bool flag = false;
    for (size_t i = 0; i < cids_.size(); i++) {
      DataType type = vals_.schema().TypeOf(i);
      switch (ops_[i]) {
        case Equal:
          flag = record::Equal(type, r, vals_, i);
          break;
        case LessThan:
          flag = record::LessThan(type, r, vals_, i);
          break;
        case LessThanOrEqual:
          flag = record::LessThanOrEqual(type, r, vals_, i);
          break;
        case GreaterThan:
          flag = record::GreaterThan(type, r, vals_, i);
          break;
        case GreaterThanOrEqual:
          flag = record::GreaterThanOrEqual(type, r, vals_, i);
          break;
        case NotEqual:
          flag = record::NotEqual(type, r, vals_, i);
          break;
        default:
          LOG(FATAL) << "unimplemented!";
      }
      if (!flag) {
        break;
      }
    }
    if (flag) {
      out_rs.push_back(std::move(r));
    }
  }

  return true;
}

}  // namespace dataflow
}  // namespace pelton
