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

    std::pair<Key, bool> key = r.GetKey();
    if (!contents_.contains(key.first)) {
      // need to add key -> records entry as empty vector
      std::vector<Record> v;
      contents_.emplace(key.first, v);
    }
    std::vector<Record>& v = contents_[key.first];
    if (r.positive()) {
      v.push_back(r);
    } else {
      auto it = std::find(std::begin(v), std::end(v), r);
      v.erase(it);
    }
  }

  return true;
}

std::vector<Record> MatViewOperator::lookup(const Key& key) const {
  if (contents_.contains(key)) {
    return contents_.at(key);
  } else {
    return std::vector<Record>();
  }
}

std::vector<Record> MatViewOperator::multi_lookup(
    const std::vector<Key>& keys) {
  std::vector<Record> out;
  for (Key key : keys) {
    if (contents_.contains(key)) {
      for (auto i : contents_.at(key)) {
        out.push_back(i);
      }
    }
  }
  return out;
}

}  // namespace dataflow
