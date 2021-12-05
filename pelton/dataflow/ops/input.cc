#include "pelton/dataflow/ops/input.h"

#include <utility>

#include "glog/logging.h"

namespace pelton {
namespace dataflow {

std::vector<Record> InputOperator::Process(NodeIndex source,
                                           std::vector<Record> &&records,
                                           const Promise &promise) {
  // Validate input records have correct schemas.
  for (const Record &record : records) {
    if (record.schema() != this->input_schemas_.at(0)) {
      LOG(FATAL) << "Input record has bad schema";
    }
  }
  return std::move(records);
}

std::unique_ptr<Operator> InputOperator::Clone() const {
  return std::make_unique<InputOperator>(this->input_name_,
                                         this->input_schemas_.at(0));
}

}  // namespace dataflow
}  // namespace pelton
