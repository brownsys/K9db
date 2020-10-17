#include "dataflow/ops/filter.h"

#include <utility>
#include <vector>

#include "dataflow/record.h"

namespace dataflow {

// performs logical AND on the predicates(cid + comparision ooperator + value)
// with their corresponding columns
bool FilterOperator::process(std::vector<Record>& rs,
                             std::vector<Record>& out_rs) {
  assert(cid_.size() == op_.size() == val_.size());

  for (Record& r : rs) {
    bool flag;
    for (int i = 0; i < cid_.size(); i++) {
      RecordData entry = *((const RecordData*)r[cid[i]]);
      switch (op_[i]) {
        case OpsEq:
          flag = entry.as_val_ ==
                 val[i].as_val_;  // if(entry.as_val_ == val[i].as_val_){
                                  // out_rs.push_back(std::move(r)); }
          break;
        case OpsLT:
          flag = entry.as_val_ < val[i].as_val_;
          break;
        case OpsLT_Eq:
          flag = entry.as_val_ <= val[i].as_val_;
          break;
        case OpsGT:
          flag = entry.as_val_ > val[i].as_val_;
          break;
        case OpsGT_Eq:
          flag = entry.as_val_ >= val[i].as_val_;
          break;
        case OpsN_Eq:
          flag = entry.as_val_ != val[i].as_val_;
          break;
      }
      if (!flag) {
        break;
      }
    }
    if (!flag) {
      continue;
    }
    out_rs.push_back(std::move(r));
  }

  return true;
}

}  // namespace dataflow
