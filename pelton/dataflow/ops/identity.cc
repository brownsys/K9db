#include "pelton/dataflow/ops/identity.h"

#include <memory>
#include <vector>

#include "glog/logging.h"
#include "pelton/dataflow/graph.h"
#include "pelton/dataflow/record.h"

namespace pelton {
namespace dataflow {

void IdentityOperator::ComputeOutputSchema() {
  this->output_schema_ = this->input_schemas_.at(0);
}

std::optional<std::vector<Record>> IdentityOperator::Process(
    NodeIndex /*source*/, const std::vector<Record>& /*records*/) {
  return std::nullopt;
}

std::shared_ptr<Operator> IdentityOperator::Clone() const {
  auto clone = std::make_shared<IdentityOperator>();
  clone->children_ = this->children_;
  clone->parents_ = this->parents_;
  clone->input_schemas_ = this->input_schemas_;
  clone->output_schema_ = this->output_schema_;
  clone->index_ = this->index_;
  return clone;
}

}  // namespace dataflow
}  // namespace pelton
