#include "pelton/dataflow/ops/union.h"

#include <utility>

#include "glog/logging.h"

namespace pelton {
namespace dataflow {

bool UnionOperator::DeepCompareSchemas(const SchemaRef s1, const SchemaRef s2) {
  if (s1.column_names() != s2.column_names()) {
    return false;
  }
  if (s1.column_types() != s2.column_types()) {
    return false;
  }
  if (s1.keys() != s2.keys()) {
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

std::vector<Record> UnionOperator::Process(NodeIndex source,
                                           std::vector<Record> &&records,
                                           const Promise &promise) {
  return std::move(records);
}

std::unique_ptr<Operator> UnionOperator::Clone() const {
  return std::make_unique<UnionOperator>();
}

}  // namespace dataflow
}  // namespace pelton
