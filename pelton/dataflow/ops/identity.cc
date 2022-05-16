#include "pelton/dataflow/ops/identity.h"

#include <utility>

namespace pelton {
namespace dataflow {

void IdentityOperator::ComputeOutputSchema() {
  this->output_schema_ = this->input_schemas_.at(0);
}

std::vector<Record> IdentityOperator::Process(NodeIndex source,
                                              std::vector<Record>&& records,
                                              const Promise& promise) {
  return std::move(records);
}

std::vector<Record> IdentityOperator::ProcessDP(
    NodeIndex source, std::vector<Record> &&records, const Promise &promise,
    pelton::dp::DPOptions *dp_options) {
  LOG(FATAL) << "ProcessDP should only be called from an AggregateOperator.";
}

std::unique_ptr<Operator> IdentityOperator::Clone() const {
  return std::make_unique<IdentityOperator>();
}

}  // namespace dataflow
}  // namespace pelton
