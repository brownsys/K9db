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

bool IdentityOperator::Process(NodeIndex source,
                               const std::vector<Record> &records,
                               std::vector<Record> *output) {
  LOG(FATAL) << "Process() called on IdentityOperator";
  return false;
}

bool IdentityOperator::ProcessAndForward(NodeIndex source,
                                         const std::vector<Record> &records) {
  for (NodeIndex childIndex : this->children_) {
    std::shared_ptr<Operator> child = this->graph()->GetNode(childIndex);
    if (!child->ProcessAndForward(this->index(), records)) {
      return false;
    }
  }

  return true;
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
