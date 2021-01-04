#include "dataflow/ops/matview.h"

#include <vector>

#include "dataflow/record.h"

namespace dataflow {

// Leaves out_rs empty, as materialized views are leaves of the graph.
// We may change this if we allow mat views to have descendant nodes.
bool MatViewOperator::process(std::vector<Record>& rs,
                              std::vector<Record>& out_rs) {
  for (Record& r : rs) {
    std::pair<Key, bool> key = get_key(r);
    if(!contents_.insert(key.first, r))
        return false;
  }
  return true;
}

std::vector<Record> MatViewOperator::lookup(const Key& key) const {
  return contents_.group(key);
}

std::vector<Record> MatViewOperator::multi_lookup(
    const std::vector<Key>& keys) {
  std::vector<Record> out;
  for (Key key : keys) {
    if (contents_.contains(key)) {
      // out.push_back(contents_.at(key));
      // ??? deprecated ???
    }
  }
  return out;
}

}  // namespace dataflow
