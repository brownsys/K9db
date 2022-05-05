#include "pelton/dataflow/ops/forward_view.h"

#include <utility>

#include "glog/logging.h"

namespace pelton {
namespace dataflow {

void ForwardViewOperator::ComputeOutputSchema() {
  for (const auto &schema : this->input_schemas_) {
    CHECK_EQ(schema, this->output_schema_);
  }
}

std::vector<Record> ForwardViewOperator::Process(NodeIndex source,
                                                std::vector<Record>&& records,
                                                const Promise& promise) {
  return std::move(records);
}

std::unique_ptr<Operator> ForwardViewOperator::Clone() const {
  return std::make_unique<ForwardViewOperator>(this->output_schema_,
                                               this->parent_flow_,
                                               this->parent_id_);
}

}  // namespace dataflow
}  // namespace pelton
