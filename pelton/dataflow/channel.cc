#include "pelton/dataflow/channel.h"

#include <utility>

namespace pelton {
namespace dataflow {

// Channel constructor.
Channel::Channel(size_t workers) : queues_(workers), semaphore_(0) {}

// Called by consumer.
std::vector<Channel::Message> Channel::Read() {
  // Block until producer notify.
  this->semaphore_.acquire();
  // Consumer work up. Lock all the queues (and the semaphore).
  std::unique_lock<std::shared_mutex> lock(this->mtx_);
  // All producers are locked out from all members here.
  // Step 1: Consume all notifications because we will read all the queues.
  while (this->semaphore_.try_acquire()) {
  }
  // Step 2: Read all the queues.
  std::vector<Channel::Message> result;
  for (std::deque<Channel::Message> &queue : this->queues_) {
    while (!queue.empty()) {
      Channel::Message &m = queue.front();
      result.emplace_back(std::move(m));
      queue.pop_front();
    }
  }
  while (!this->input_queue_.empty()) {
    Channel::Message &m = this->input_queue_.front();
    result.emplace_back(std::move(m));
    this->input_queue_.pop_front();
  }
  return result;
}

// Called by (worker) producers.
void Channel::Write(PartitionIndex producer, Channel::Message &&msg) {
  std::shared_lock<std::shared_mutex> produer_lock(this->mtx_);
  // Other producers might be here, but not the consumer.
  // Only one producer with this index can be active, since the index
  // is tied to the worker thread.
  std::deque<Channel::Message> &queue = this->queues_.at(producer);
  queue.push_back(std::move(msg));
  this->semaphore_.release();
}

// Called by (client) producers.
void Channel::WriteInput(Channel::Message &&msg) {
  std::shared_lock<std::shared_mutex> produer_lock(this->mtx_);
  // Many producers might be here, but not the consumer.
  std::unique_lock<std::mutex> input_lock(this->input_mtx_);
  // Only one input producer, input_queue_ is safe!
  this->input_queue_.push_back(std::move(msg));
  this->semaphore_.release();
}

void Channel::Shutdown() {
  std::shared_lock<std::shared_mutex> produer_lock(this->mtx_);
  this->semaphore_.release();
}

}  // namespace dataflow
}  // namespace pelton
