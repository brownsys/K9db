#include "dataflow/ops/filter.h"

#include <utility>
#include <vector>

#include "dataflow/record.h"

namespace dataflow {

bool FilterOperator::process(std::vector<Record>& rs,
                               std::vector<Record>& out_rs) {

  for (Record& r : rs) {
    RecordData entry = *((const RecordData*)r[cid]);
    switch(op_){
      case "==":
        if(entry.as_val_ == val.as_val_){ out_rs.push_back(std::move(r)); }
        break;
    }
  };

  return true;
}

}  // namespace dataflow
