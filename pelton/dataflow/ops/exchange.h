#ifndef PELTON_DATAFLOW_OPS_EXCHANGE_H_
#define PELTON_DATAFLOW_OPS_EXCHANGE_H_

#include <memory>
#include <vector>

#include "pelton/dataflow/graph.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

class ExchangeOperator : public Operator {
 public:
  // Cannot copy an operator.
  ExchangeOperator(const ExchangeOperator &other) = delete;
  ExchangeOperator &operator=(const ExchangeOperator &other) = delete;

  ExchangeOperator(DataFlowGraph *graph, const std::vector<ColumnID> key)
      : Operator(Operator::Type::EXCHANGE), graph_(graph), key_(key) {}

  const std::vector<ColumnID> &key() const { return this->key_; }

 protected:
  std::vector<Record> Process(NodeIndex source,
                              std::vector<Record> &&records) override;

  void ComputeOutputSchema() override;

  std::unique_ptr<Operator> Clone() const override;

 private:
  DataFlowGraph *graph_;
  std::vector<ColumnID> key_;  // partitioning key.
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_EXCHANGE_H_
