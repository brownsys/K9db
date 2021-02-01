#include "dataflow/ops/matview.h"

#include <vector>

#include "dataflow/record.h"

namespace dataflow {

// Leaves out_rs empty, as materialized views are leaves of the graph.
// We may change this if we allow mat views to have descendant nodes.
bool MatViewOperator::process(std::vector<Record>& rs,
                              std::vector<Record>& out_rs) {
  for (Record& r : rs) {
    // incoming records must have the right key column set
    CHECK(r.schema().key_columns() == key_cols_);

    //    std::pair<Key, bool> key = r.GetKey();
    //    if (!contents_.contains(key.first)) {
    //      // need to add key -> records entry as empty vector
    //      std::vector<Record> v;
    //      contents_.insert(key.first, v);
    //    }
    //    std::vector<Record>& v = contents_.group(key.first);
    //    if (r.positive()) {
    //      v.push_back(r);
    //    } else {
    //      auto it = std::find(std::begin(v), std::end(v), r);
    //      v.erase(it);
    //    }
    std::pair<Key, bool> key = r.GetKey();
    if (!contents_.insert(key.first, r)) return false;
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
