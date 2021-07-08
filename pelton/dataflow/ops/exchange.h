#ifndef PELTON_DATAFLOW_OPS_EXCHANGE_H_
#define PELTON_DATAFLOW_OPS_EXCHANGE_H_

#include <memory>
#include <thread>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "gtest/gtest_prod.h"
#include "pelton/dataflow/batch_message.h"
#include "pelton/dataflow/channel.h"
#include "pelton/dataflow/graph.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/partition.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

class ExchangeOperator : public Operator {
 public:
  ExchangeOperator(
      std::shared_ptr<Channel> incoming_chan,
      std::shared_ptr<Channel> graph_chan,
      absl::flat_hash_map<uint16_t, std::shared_ptr<Channel>> peers,
      std::vector<ColumnID> partition_key, uint16_t current_partition,
      uint16_t total_partitions)
      : Operator(Operator::Type::EXCHANGE),
        incoming_chan_(incoming_chan),
        graph_chan_(graph_chan),
        peers_(peers),
        partition_key_(partition_key),
        current_partition_(current_partition),
        total_partitions_(total_partitions),
        listen_thread_(
            std::move(std::thread(&ExchangeOperator::ListenFromPeers, this))) {}
  ~ExchangeOperator() {
    // Ensure that the thread exits gracefully
    this->listen_thread_.join();
  }

 protected:
  bool Process(NodeIndex source, const std::vector<Record> &records,
               std::vector<Record> *output) override;

  void ComputeOutputSchema() override;

  FRIEND_TEST(ExchangeOperatorTest, BasicTest);

 private:
  // Used by peers of this operator to send messages to this operator
  std::shared_ptr<Channel> incoming_chan_;
  // Used to send messages to the "main" graph thread of this partition
  std::shared_ptr<Channel> graph_chan_;
  // Used to send messages to any of it's peers
  absl::flat_hash_map<uint16_t, std::shared_ptr<Channel>> peers_;
  // Key cols that this exchange operator is supposed to partition records by
  std::vector<ColumnID> partition_key_;
  uint16_t current_partition_;
  uint16_t total_partitions_;
  std::thread listen_thread_;

  void ListenFromPeers() {
    // NOTE: This thread does not process records via recursive function
    // calls because that would require locks on operators' state (if any).
    // Instead this thread forwards sends the records for processing to the
    // "main"(w.r.t the partition) thread via graph_chan_.
    while (true) {
      std::vector<std::shared_ptr<Message>> messages =
          this->incoming_chan_->Recv();
      // Forward each message to the graph channel
      for (auto const msg : messages) {
        switch (msg->type()) {
          case Message::Type::BATCH: {
            auto batch_msg = std::dynamic_pointer_cast<BatchMessage>(msg);
            // Set the "entry" node for the message as the child of this
            // exchange operator.
            batch_msg->entry_index =
                this->graph()->GetNode(this->children_.at(0))->index();
            // Set the source as the current node's index.
            batch_msg->source_index = this->index();
            this->graph_chan_->Send(batch_msg);
          } break;
          case Message::Type::STOP:
            // Stop execution of this thread so that it can exit gracefully and
            // the test suite deems the test as a success.
            return;
          default:
            LOG(FATAL) << "Invalid message type";
        }
      }
    }
  }

  virtual std::shared_ptr<Operator> Clone() const {
    LOG(FATAL) << "Exchange operator does not support clone";
  };
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_EXCHANGE_H_
