#include "dataflow/ops/matview.h"

#include <vector>

#include "dataflow/record.h"

namespace dataflow {

bool MatViewOperator::process(std::vector<Record>& rs) {
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
