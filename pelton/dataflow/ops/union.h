#ifndef PELTON_DATAFLOW_OPS_UNION_H_
#define PELTON_DATAFLOW_OPS_UNION_H_

#include <vector>

#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

class UnionOperator : public Operator {
 public:
  UnionOperator() : Operator(Operator::Type::UNION) {}

  std::optional<std::vector<Record>> Process(
      NodeIndex, const std::vector<Record> &records) override;

 protected:
  bool DeepCompareSchemas(const SchemaRef s1, const SchemaRef s2);
  void ComputeOutputSchema() override;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_UNION_H_
