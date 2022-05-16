#ifndef PELTON_DATAFLOW_OPS_UNION_H_
#define PELTON_DATAFLOW_OPS_UNION_H_

#include <memory>
#include <vector>

#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {
class UnionOperator : public Operator {
 public:
  // Cannot copy an operator.
  UnionOperator(const UnionOperator &other) = delete;
  UnionOperator &operator=(const UnionOperator &other) = delete;

  UnionOperator() : Operator(Operator::Type::UNION) {}

 protected:
  bool DeepCompareSchemas(const SchemaRef s1, const SchemaRef s2);

  std::vector<Record> Process(NodeIndex source, std::vector<Record> &&records,
                              const Promise &promise) override;

  std::vector<Record> ProcessDP(NodeIndex source, std::vector<Record> &&records,
                                const Promise &promise,
                                pelton::dp::DPOptions *dp_options) override;

  void ComputeOutputSchema() override;

  std::unique_ptr<Operator> Clone() const override;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_UNION_H_
