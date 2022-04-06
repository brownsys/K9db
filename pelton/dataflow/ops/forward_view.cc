#include "pelton/dataflow/ops/forward_view.h"

#include <utility>

namespace pelton {
namespace dataflow {

void ForwardViewOperator::ComputeOutputSchema() {
  this->output_schema_ = this->input_schemas_.at(0);
}

std::vector<Record> ForwardViewOperator::Process(NodeIndex source,
                                                std::vector<Record>&& records,
                                                const Promise& promise) {
  return std::move(records);
}

std::unique_ptr<Operator> ForwardViewOperator::Clone() const {
  return std::make_unique<ForwardViewOperator>();
}

}  // namespace dataflow
}  // namespace pelton
