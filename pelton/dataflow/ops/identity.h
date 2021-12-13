#ifndef PELTON_DATAFLOW_OPS_IDENTITY_H_
#define PELTON_DATAFLOW_OPS_IDENTITY_H_

#include <memory>
#include <vector>

#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

class IdentityOperator : public Operator {
 public:
  // Cannot copy an operator.
  IdentityOperator(const IdentityOperator &other) = delete;
  IdentityOperator &operator=(const IdentityOperator &other) = delete;

  IdentityOperator() : Operator(Operator::Type::IDENTITY) {}

 protected:
  std::vector<Record> Process(NodeIndex source, std::vector<Record> &&records,
                              const Promise &promise) override;

  void ComputeOutputSchema() override;

  std::unique_ptr<Operator> Clone() const override;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_IDENTITY_H_
