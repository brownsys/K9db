#include "pelton/dataflow/ops/input.h"

#include <memory>

#include "glog/logging.h"
#include "pelton/dataflow/record.h"

namespace pelton {
namespace dataflow {

bool InputOperator::Process(NodeIndex source,
                            const std::vector<Record> &records,
                            std::vector<Record> *output) {
  LOG(FATAL) << "Process() called on InputOperator";
  return false;
}

bool InputOperator::ProcessAndForward(NodeIndex source,
                                      const std::vector<Record> &records) {
  // Validate input records have correct schemas.
  for (const Record &record : records) {
    if (record.schema() != this->input_schemas_.at(0)) {
      LOG(FATAL) << "Input record has bad schema";
    }
  }

  // Forward input records to children.
  for (NodeIndex childIndex : this->children_) {
    std::shared_ptr<Operator> child = this->graph_()->GetNode(childIndex);
    if (!child->ProcessAndForward(this->index(), records)) {
      return false;
    }
  }

  return true;
}

}  // namespace dataflow
}  // namespace pelton
