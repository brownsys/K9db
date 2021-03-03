#include "pelton/dataflow/ops/matview.h"

#include <vector>

#include "glog/logging.h"

namespace pelton {
namespace dataflow {

// Explicitly specified keys for this Materialized view may differ from
// PrimaryKey.
MatViewOperator::MatViewOperator(const std::vector<ColumnID> &key_cols)
    : Operator(Operator::Type::MAT_VIEW), key_cols_(key_cols) {
  CHECK(!key_cols_.empty()) << "Matview keys cannot be empty";
}

MatViewOperator::MatViewOperator(std::vector<ColumnID> &&key_cols)
    : Operator(Operator::Type::MAT_VIEW), key_cols_(std::move(key_cols)) {
  CHECK(!key_cols_.empty()) << "Matview keys cannot be empty";
}

// Compute the output schema when the input schema is available.
void MatViewOperator::ComputeOutputSchema() {
  this->output_schema_ = this->input_schemas_.at(0);
}

// Leaves output empty, as materialized views are leaves of the graph.
// We may change this if we allow mat views to have descendant nodes.
bool MatViewOperator::Process(NodeIndex source,
                              const std::vector<Record> &records,
                              std::vector<Record> *output) {
  for (const Record &r : records) {
    if (!this->contents_.Insert(r.GetValues(this->key_cols_), r)) {
      return false;
    }
  }

  return true;
}

size_t MatViewOperator::count() const { return this->contents_.count(); }
bool MatViewOperator::Contains(const Key &key) const {
  return this->contents_.Contains(key);
}

const std::vector<Record> &MatViewOperator::Lookup(const Key &key) const {
  return this->contents_.Get(key);
}

GroupedData::const_iterator MatViewOperator::begin() const {
  return this->contents_.begin();
}

GroupedData::const_iterator MatViewOperator::end() const {
  return this->contents_.end();
}

}  // namespace dataflow
}  // namespace pelton
