#ifndef PELTON_DATAFLOW_CHANNEL_H_
#define PELTON_DATAFLOW_CHANNEL_H_

#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <vector>

#include "pelton/dataflow/message.h"

namespace pelton {
namespace dataflow {

// Forward declarations
class Worker;

class Channel {
 public:
  explicit Channel(std::shared_ptr<std::condition_variable> not_empty,
                   std::weak_ptr<Worker> destination_worker,
                   uint64_t capacity = 10000)
      : not_empty_(not_empty),
        destination_worker_(destination_worker),
        capacity_(capacity) {}
  // Cannot copy a channel.
  Channel(const Channel &other) = delete;
  Channel &operator=(const Channel &other) = delete;

  ~Channel() {
    std::unique_lock<std::mutex> lock(mtx_);
    // Ensure that all messages have been processed.
    CHECK_EQ(this->queue_.size(), (size_t)0);
  }

  bool Send(std::shared_ptr<Message> message);

  // The queue gets flushed since we are following a single producer-single
  // consumer pattern
  std::optional<std::vector<std::shared_ptr<Message>>> Recv();

  size_t size() {
    mtx_.lock();
    size_t size = queue_.size();
    mtx_.unlock();
    return size;
  }

  void SetGraphIndex(GraphIndex graph_index) {
    this->graph_index_ = graph_index;
  }
  void SetSourceIndex(NodeIndex source_index) {
    this->source_index_ = source_index;
  }
  void SetDestinationIndex(NodeIndex destination_index) {
    this->destination_index_ = destination_index;
  }
  // Accessors
  const GraphIndex &graph_index() { return this->graph_index_; }
  const std::optional<NodeIndex> source_index() { return this->source_index_; }
  const NodeIndex destination_index() { return this->destination_index_; }

 private:
  std::deque<std::shared_ptr<Message>> queue_;
  std::mutex mtx_;
  std::shared_ptr<std::condition_variable> not_empty_;
  std::condition_variable not_full_;
  // Graph/flow that this channel belongs to.
  GraphIndex graph_index_;
  std::weak_ptr<Worker> destination_worker_;
  uint64_t capacity_;
  // If @source_index_ is null it implies that the records are meant for an
  // input operator and are being sent by the dataflow engine. Else they are
  // being sent by an exchange operator.
  std::optional<NodeIndex> source_index_;
  NodeIndex destination_index_;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_CHANNEL_H_
