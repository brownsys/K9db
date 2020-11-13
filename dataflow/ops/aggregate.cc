#include "dataflow/ops/aggregate.h"

#include <vector>

#include "dataflow/record.h"

namespace dataflow {

bool AggregateOperator::process(std::vector<Record>& rs,
                                std::vector<Record>& out_rs) {
  for (Record& r : rs) {
    std::vector<RecordData> key = get_key(r);
    if (!state_.contains(key)) {
      switch (agg_func_) {
        case FuncMax:
          state_.emplace(key, r.raw_at(agg_col_).as_val());
          break;
      }
    } else {
      switch (agg_func_) {
        case FuncMax:
          if (r.raw_at(agg_col_).as_val() > state_.at(key)) {
            state_.at(key) = r.raw_at(agg_col_).as_val();
          }
          break;
      }
    }
  }

  for (auto const& item : state_) {
    std::vector<RecordData> temp = item.first;
    temp.push_back(RecordData(item.second));
    out_rs.push_back(Record(true, temp, 3ULL));
  }

  return true;
}

}  // namespace dataflow
