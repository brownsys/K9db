#ifndef K9DB_DATAFLOW_OPS_IDENTITY_H_
#define K9DB_DATAFLOW_OPS_IDENTITY_H_

#include <memory>
#include <vector>

#include "k9db/dataflow/operator.h"
#include "k9db/dataflow/record.h"
#include "k9db/dataflow/types.h"

namespace k9db {
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
}  // namespace k9db

#endif  // K9DB_DATAFLOW_OPS_IDENTITY_H_
