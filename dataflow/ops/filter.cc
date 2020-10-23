#include "dataflow/ops/filter.h"

#include <utility>
#include <vector>

#include "dataflow/record.h"

namespace dataflow {

// performs logical AND on the predicates(cid + comparision ooperator + value)
// with their corresponding columns
bool FilterOperator::process(std::vector<Record>& rs,
                             std::vector<Record>& out_rs) {
  std::vector<ColumnID> cids = getCid();
  std::vector<Ops> ops = getOp();
  std::vector<RecordData> vals = getVal();

  for (Record& r : rs) {
    bool flag = false;
    for (size_t i = 0; i < cids.size(); i++) {
      RecordData entry = r.raw_at(cids[i]);
      switch (ops[i]) {
        case OpsEq:
          flag = entry.as_val() == vals[i].as_val();
          break;
        case OpsLT:
          flag = entry.as_val() < vals[i].as_val();
          break;
        case OpsLT_Eq:
          flag = entry.as_val() <= vals[i].as_val();
          break;
        case OpsGT:
          flag = entry.as_val() > vals[i].as_val();
          break;
        case OpsGT_Eq:
          flag = entry.as_val() >= vals[i].as_val();
          break;
        case OpsN_Eq:
          flag = entry.as_val() != vals[i].as_val();
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
