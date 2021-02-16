#include "pelton/dataflow/ops/union.h"

#include <memory>
#include <vector>

#include "glog/logging.h"
#include "pelton/dataflow/record.h"

namespace pelton {
namespace dataflow {

bool UnionOperator::Process(NodeIndex source,
                            const std::vector<Record> &records,
                            std::vector<Record> *output) {
  LOG(FATAL) << "Process() called on UnionOperator";
  return false;
}

bool UnionOperator::ProcessAndForward(NodeIndex source,
                                      const std::vector<Record> &records) {
  for (std::weak_ptr<Edge> edge_ptr : this->children_) {
    std::shared_ptr<Edge> edge = edge_ptr.lock();
    std::shared_ptr<Operator> child = edge->to().lock();
    if (!child->ProcessAndForward(this->index(), records)) {
      return false;
    }
  }

  return true;
}

}  // namespace dataflow
}  // namespace pelton
