#include "pelton/dataflow/ops/union.h"

#include <memory>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "pelton/dataflow/graph.h"
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

std::vector<Record> UnionOperator::Process(NodeIndex source,
                                           std::vector<Record> &&records) {
  return std::move(records);
}

std::shared_ptr<Operator> UnionOperator::Clone() const {
  auto clone = std::make_shared<UnionOperator>();
  clone->children_ = this->children_;
  clone->parents_ = this->parents_;
  clone->input_schemas_ = this->input_schemas_;
  clone->output_schema_ = this->output_schema_;
  return clone;
}

}  // namespace dataflow
}  // namespace pelton
