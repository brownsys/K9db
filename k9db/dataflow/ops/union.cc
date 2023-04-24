#include "k9db/dataflow/ops/union.h"

#include <utility>

#include "glog/logging.h"

namespace k9db {
namespace dataflow {

namespace {

bool EqualWithoutKeys(const SchemaRef &s1, const SchemaRef &s2) {
  if (s1.column_names() != s2.column_names()) {
    return false;
  }
  if (s1.column_types() != s2.column_types()) {
    return false;
  }
  return true;
}

SchemaRef ChooseKeys(const SchemaRef &s1, const SchemaRef &s2) {
  if (s1.keys() == s2.keys()) {
    return s1;
  }
  return SchemaFactory::Create(s1.column_names(), s1.column_types(), {});
}

}  // namespace

void UnionOperator::ComputeOutputSchema() {
  this->output_schema_ = this->input_schemas_.at(0);
  for (const SchemaRef &input_schema : this->input_schemas_) {
    CHECK(EqualWithoutKeys(this->output_schema_, input_schema))
        << "UnionOperator has inputs with different schemas: "
        << this->output_schema_ << " != " << input_schema;
    this->output_schema_ = ChooseKeys(this->output_schema_, input_schema);
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
}  // namespace k9db
