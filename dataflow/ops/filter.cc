#include "dataflow/ops/filter.h"

#include <utility>
#include <vector>

#include "dataflow/record.h"
#include "dataflow/record_utils.h"

namespace dataflow {

bool FilterOperator::process(std::vector<Record>& rs,
                             std::vector<Record>& out_rs) {
  for (Record& r : rs) {
    bool flag = false;
    for (size_t i = 0; i < cids_.size(); i++) {
      DataType type = vals_.schema().TypeOf(i);
      switch (ops_[i]) {
        case Equal:
          flag = Eq(type, r, vals_, i);
          break;
        case LessThan:
          flag = Lt(type, r, vals_, i);
          break;
        case LessThanOrEqual:
          flag = LtEq(type, r, vals_, i);
          break;
        case GreaterThan:
          flag = Gt(type, r, vals_, i);
          break;
        case GreaterThanOrEqual:
          flag = GtEq(type, r, vals_, i);
          break;
        case NotEqual:
          flag = NotEq(type, r, vals_, i);
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
