#include "pelton/dataflow/ops/union.h"

#include <memory>
#include <vector>

#include "glog/logging.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"

namespace pelton {
namespace dataflow {

bool UnionOperator::DeepCompareSchemas(const SchemaRef s1, const SchemaRef s2) {
  if (s1.column_names() != s2.column_names()) {
    return false;
  }
  if (s1.column_types() != s2.column_types()) {
    return false;
  }
  return true;
}

void UnionOperator::ComputeOutputSchema() {
  this->output_schema_ = this->input_schemas_.at(0);
  for (const SchemaRef &input_schema : this->input_schemas_) {
    if (!DeepCompareSchemas(this->output_schema_, input_schema)) {
      LOG(FATAL) << "UnionOperator has inputs with different schemas";
    }
  }
}

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
