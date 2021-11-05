#ifndef PELTON_DATAFLOW_OPS_EXCHANGE_H_
#define PELTON_DATAFLOW_OPS_EXCHANGE_H_

#include <memory>
#include <string>
#include <vector>

#include "pelton/dataflow/graph_partition.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

class DataFlowGraph;

class ExchangeOperator : public Operator {
 public:
  // Cannot copy an operator.
  ExchangeOperator(const ExchangeOperator &other) = delete;
  ExchangeOperator &operator=(const ExchangeOperator &other) = delete;

  ExchangeOperator(
      std::vector<std::unique_ptr<DataFlowGraphPartition>> *partitions,
      const PartitionKey &outkey)
      : Operator(Operator::Type::EXCHANGE),
        partitions_(partitions),
        outkey_(outkey) {}

  const std::vector<ColumnID> &outkey() const { return this->outkey_; }

  std::string DebugString() const override {
    std::string result = Operator::DebugString();
    result += "  \"outkey\": [";
    for (ColumnID key : this->outkey_) {
      result += std::to_string(key) + ", ";
    }
    if (this->outkey_.size() > 0) {
      result.pop_back();
      result.pop_back();
    }
    result += "],\n";
    return result;
  }

 protected:
  std::vector<Record> Process(NodeIndex source,
                              std::vector<Record> &&records) override;

  void ComputeOutputSchema() override;

  std::unique_ptr<Operator> Clone() const override;

 private:
  std::vector<std::unique_ptr<DataFlowGraphPartition>> *partitions_;
  PartitionKey outkey_;  // partitioning key.
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_EXCHANGE_H_
