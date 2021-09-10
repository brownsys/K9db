#include "pelton/dataflow/channel.h"

#include "pelton/dataflow/worker.h"

namespace pelton {
namespace dataflow {

bool Channel::Send(std::shared_ptr<Message> message) {
  std::unique_lock<std::mutex> lock(mtx_);
  while (queue_.size() == capacity_) {
    not_full_.wait(lock);
  }
  queue_.push_back(message);
  // The following explicit notification has to be performed so that the
  // worker does not miss a notification in case it is busy reading from
  // other channels.
  if (auto sp = this->destination_worker_.lock()) {
    sp->NotifyWorker();
  } else {
    LOG(FATAL) << "Destination worker does not exist";
  }
  // Only a single thread (that is responsible for nth partitions of all
  // flows) is supposed to be waiting on this condition variable. Hence only
  // one threads ends up getting notified.
  not_empty_->notify_all();
  return true;
}

std::optional<std::vector<std::shared_ptr<Message>>> Channel::Recv() {
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

}  // namespace dataflow
}  // namespace pelton
