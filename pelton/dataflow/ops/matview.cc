#include "pelton/dataflow/ops/matview.h"

#include <utility>
#include <vector>

#include "glog/logging.h"

namespace pelton {
namespace dataflow {

// Leaves out_rs empty, as materialized views are leaves of the graph.
// We may change this if we allow mat views to have descendant nodes.
bool MatViewOperator::Process(NodeIndex source,
                              const std::vector<Record> &records,
                              std::vector<Record> *output) {
  for (const Record &r : records) {
    // incoming records must have the right key column set
    CHECK(r.schema().keys() == this->key_cols_);  // TODO(babman): order?

    if (!this->contents_.Insert(r)) {
      return false;
    }
  }

  return true;
}

const std::vector<Record> &MatViewOperator::Lookup(const Key &key) const {
  return this->contents_.Get(key);
}

GroupedData::const_iterator MatViewOperator::cbegin() const {
  return this->contents_.cbegin();
}

GroupedData::const_iterator MatViewOperator::cend() const {
  return this->contents_.cend();
}

}  // namespace dataflow
}  // namespace pelton
