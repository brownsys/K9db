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

}  // namespace dataflow
}  // namespace pelton
