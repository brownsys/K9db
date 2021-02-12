#include "pelton/dataflow/ops/matview.h"

#include <utility>
#include <vector>

#include "pelton/dataflow/record.h"

namespace pelton {
namespace dataflow {

// Leaves out_rs empty, as materialized views are leaves of the graph.
// We may change this if we allow mat views to have descendant nodes.
bool MatViewOperator::process(std::vector<Record>& rs,
                              std::vector<Record>& out_rs) {
  for (Record& r : rs) {
    // incoming records must have the right key column set
    CHECK(r.schema().key_columns() == key_cols_);

    std::pair<Key, bool> key = r.GetKey();
    if (!contents_.insert(key.first, r)) return false;
  }

  return true;
}

std::vector<Record> MatViewOperator::lookup(const Key& key) const {
  return contents_.group(key);
}

std::vector<Record> MatViewOperator::contents() const {
  return contents_.contents();
}

}  // namespace dataflow
}  // namespace pelton
