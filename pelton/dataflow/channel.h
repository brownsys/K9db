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

class Channel {
 public:
  explicit Channel(std::shared_ptr<std::condition_variable> not_empty,
                   uint64_t capacity = 10000)
      : not_empty_(not_empty), capacity_(capacity) {}
  // Cannot copy a channel.
  Channel(const Channel &other) = delete;
  Channel &operator=(const Channel &other) = delete;

  bool Send(std::shared_ptr<Message> message) {
    std::unique_lock<std::mutex> lock(mtx_);
    while (queue_.size() == capacity_) {
      not_full_.wait(lock);
    }
    queue_.push_back(message);
    // Only a single thread (that is responsible for nth partitions of all
    // flows) is supposed to be waiting on this condition variable. Hence only
    // one threads ends up getting notified.
    not_empty_->notify_all();
    return true;
  }

  // The queue gets flushed since we are following a single producer-single
  // consumer pattern
  std::optional<std::vector<std::shared_ptr<Message>>> Recv() {
    std::unique_lock<std::mutex> lock(mtx_);
    // TODO(Ishan): It may be a little unsafe but consider performing the queue
    // empty check without grabbing the above lock.
    if (queue_.size() == 0) {
      // In order to support reading from multiple channels, reads from empty
      // channels do not block.
      return std::nullopt;
    }
    std::vector<std::shared_ptr<Message>> messages;
    while (!queue_.empty()) {
      messages.push_back(queue_.front());
      queue_.pop_front();
    }
    // Since the channel gets flushed upon each Recv, most likely the Send
    // requests of all the threads that are sleeping could be accomodated.
    not_full_.notify_all();
    return messages;
  }

  size_t size() {
    mtx_.lock();
    size_t size = queue_.size();
    mtx_.unlock();
    return size;
  }

  void SetSourceIndex(NodeIndex source_index) {
    this->source_index_ = source_index;
  }
  void SetDestinationIndex(NodeIndex destination_index) {
    this->destination_index_ = destination_index;
  }

  // Accessors
  const std::optional<NodeIndex> source_index() { return this->source_index_; }
  const NodeIndex destination_index() { return this->destination_index_; }

 private:
  std::deque<std::shared_ptr<Message>> queue_;
  std::mutex mtx_;
  std::shared_ptr<std::condition_variable> not_empty_;
  std::condition_variable not_full_;
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
