#include "dataflow/ops/matview.h"

#include <vector>

#include "dataflow/record.h"

namespace dataflow {

// Leaves out_rs empty, as materialized views are leaves of the graph.
// We may change this if we allow mat views to have descendant nodes.
bool MatViewOperator::process(std::vector<Record>& rs,
                              std::vector<Record>& out_rs) {
  for (Record& r : rs) {
    auto key = get_key(r);
    if (!contents_.contains(key)) {
      // need to add key -> records entry
      std::vector<Record> v = {r};
      contents_.emplace(key, v);
    }
    std::vector<Record>& v = contents_[key];
    if (r.positive()) {
      v.push_back(r);
    } else {
      auto it = std::find(std::begin(v), std::end(v), r);
      v.erase(it);
    }
  }

  return true;
}

}  // namespace dataflow
