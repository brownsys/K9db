#ifndef PELTON_DATAFLOW_OPS_EXCHANGE_H_
#define PELTON_DATAFLOW_OPS_EXCHANGE_H_

#include <memory>
#include <thread>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "gtest/gtest_prod.h"
#include "pelton/dataflow/batch_message.h"
#include "pelton/dataflow/channel.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

class ExchangeOperator : public Operator {
 public:
  ExchangeOperator() = delete;
  ExchangeOperator(absl::flat_hash_map<PartitionID, std::shared_ptr<Channel>>
                       partition_chans,
                   std::vector<ColumnID> partition_key,
                   PartitionID current_partition, PartitionID total_partitions)
      : Operator(Operator::Type::EXCHANGE),
        partition_chans_(partition_chans),
        partition_key_(partition_key),
        current_partition_(current_partition),
        total_partitions_(total_partitions) {}

  std::shared_ptr<Operator> Clone() const {
    LOG(FATAL) << "Exchange operator does not support clone";
  }
  // Accessors
  const std::vector<ColumnID> &partition_key() { return this->partition_key_; }

 protected:
  std::optional<std::vector<Record>> Process(
      NodeIndex, const std::vector<Record> &records) override;

  void ComputeOutputSchema() override;

  FRIEND_TEST(ExchangeOperatorTest, BasicTest);

 private:
  // Used for sending messages to other partitions
  absl::flat_hash_map<PartitionID, std::shared_ptr<Channel>> partition_chans_;
  // Key cols that this exchange operator is supposed to partition records by
  std::vector<ColumnID> partition_key_;
  PartitionID current_partition_;
  PartitionID total_partitions_;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_EXCHANGE_H_
