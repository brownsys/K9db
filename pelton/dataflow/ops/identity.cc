#include "pelton/dataflow/ops/identity.h"

#include <memory>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "pelton/dataflow/graph.h"
#include "pelton/dataflow/record.h"

namespace pelton {
namespace dataflow {

void IdentityOperator::ComputeOutputSchema() {
  this->output_schema_ = this->input_schemas_.at(0);
}

std::vector<Record> IdentityOperator::Process(NodeIndex source,
                                              std::vector<Record> &&records) {
  return std::move(records);
}

std::shared_ptr<Operator> IdentityOperator::Clone() const {
  auto clone = std::make_shared<IdentityOperator>();
  clone->children_ = this->children_;
  clone->parents_ = this->parents_;
  clone->input_schemas_ = this->input_schemas_;
  clone->output_schema_ = this->output_schema_;
  return clone;
}

}  // namespace dataflow
}  // namespace pelton
