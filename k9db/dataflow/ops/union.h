#ifndef K9DB_DATAFLOW_OPS_UNION_H_
#define K9DB_DATAFLOW_OPS_UNION_H_

#include <memory>
#include <vector>

#include "k9db/dataflow/operator.h"
#include "k9db/dataflow/record.h"
#include "k9db/dataflow/schema.h"
#include "k9db/dataflow/types.h"

namespace k9db {
namespace dataflow {
class UnionOperator : public Operator {
 public:
  // Cannot copy an operator.
  UnionOperator(const UnionOperator &other) = delete;
  UnionOperator &operator=(const UnionOperator &other) = delete;

  UnionOperator() : Operator(Operator::Type::UNION) {}

 protected:
  std::vector<Record> Process(NodeIndex source, std::vector<Record> &&records,
                              const Promise &promise) override;

  void ComputeOutputSchema() override;

  std::unique_ptr<Operator> Clone() const override;
};

}  // namespace dataflow
}  // namespace k9db

#endif  // K9DB_DATAFLOW_OPS_UNION_H_
