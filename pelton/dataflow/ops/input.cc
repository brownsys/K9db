#include "pelton/dataflow/ops/input.h"

#include <memory>
#include <utility>

#include "glog/logging.h"
#include "pelton/dataflow/graph.h"
#include "pelton/dataflow/record.h"

namespace pelton {
namespace dataflow {

std::vector<Record> InputOperator::Process(NodeIndex source,
                                           std::vector<Record> &&records) {
  // Validate input records have correct schemas.
  for (const Record &record : records) {
    if (record.schema() != this->input_schemas_.at(0)) {
      LOG(FATAL) << "Input record has bad schema";
    }
  }
  return std::move(records);
}

std::shared_ptr<Operator> InputOperator::Clone() const {
  auto clone = std::make_shared<InputOperator>(this->input_name_,
                                               this->input_schemas_.at(0));
  clone->children_ = this->children_;
  return clone;
}

}  // namespace dataflow
}  // namespace pelton
