#ifndef K9DB_DATAFLOW_OPS_EXCHANGE_H_
#define K9DB_DATAFLOW_OPS_EXCHANGE_H_

#include <memory>
#include <string>
#include <vector>

#include "k9db/dataflow/channel.h"
#include "k9db/dataflow/operator.h"
#include "k9db/dataflow/record.h"
#include "k9db/dataflow/types.h"

namespace k9db {
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
  Record DebugRecord() const override {
    Record record = Operator::DebugRecord();
    std::string outkey = "";
    for (auto key : this->outkey_) {
      outkey += std::to_string(key) + ",";
    }
    if (this->outkey_.size() > 0) {
      outkey.pop_back();
    }
    record.SetString(std::make_unique<std::string>(outkey), 5);
    return record;
  }

 protected:
  std::vector<Record> Process(NodeIndex source, std::vector<Record> &&records,
                              const Promise &promise) override;

  void ComputeOutputSchema() override;

  std::unique_ptr<Operator> Clone() const override;

 private:
  std::string flow_name_;
  size_t partitions_;
  std::vector<Channel *> channels_;
  PartitionKey outkey_;  // partitioning key.
};

}  // namespace dataflow
}  // namespace k9db

#endif  // K9DB_DATAFLOW_OPS_EXCHANGE_H_
