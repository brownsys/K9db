#ifndef PELTON_DATAFLOW_OPS_EXCHANGE_H_
#define PELTON_DATAFLOW_OPS_EXCHANGE_H_

#include <memory>
#include <string>
#include <vector>

#include "pelton/dataflow/channel.h"
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

  ExchangeOperator(const std::string &flow_name, size_t partitions,
                   const std::vector<Channel *> &channels,
                   const PartitionKey &outkey)
      : Operator(Operator::Type::EXCHANGE),
        flow_name_(flow_name),
        partitions_(partitions),
        channels_(channels),
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
  std::string flow_name_;
  size_t partitions_;
  std::vector<Channel *> channels_;
  PartitionKey outkey_;  // partitioning key.
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_EXCHANGE_H_
