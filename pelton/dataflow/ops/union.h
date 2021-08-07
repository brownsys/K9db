#ifndef PELTON_DATAFLOW_OPS_UNION_H_
#define PELTON_DATAFLOW_OPS_UNION_H_

#include <memory>
#include <vector>

#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {
class UnionOperator : public Operator {
 public:
  // Cannot copy an operator.
  UnionOperator(const UnionOperator &other) = delete;
  UnionOperator &operator=(const UnionOperator &other) = delete;

  UnionOperator() : Operator(Operator::Type::UNION) {}

  std::optional<std::vector<Record>> Process(
      NodeIndex /*source*/, const std::vector<Record> & /*records*/) override;

  std::shared_ptr<Operator> Clone() const override;

 protected:
  bool DeepCompareSchemas(const SchemaRef s1, const SchemaRef s2);
  void ComputeOutputSchema() override;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_UNION_H_
