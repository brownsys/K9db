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
  bool Send(std::shared_ptr<Message> message) {
    mtx_.lock();
    queue_.push_back(message);
    mtx_.unlock();
    not_empty_.notify_all();
    return true;
  }

  // The queue gets flushed since we are following a multiple producer-single
  // consumer pattern
  std::vector<std::shared_ptr<Message>> Recv() {
    std::unique_lock<std::mutex> lock(mtx_);
    while (queue_.size() == 0) {
      not_empty_.wait(lock);
    }
    std::vector<std::shared_ptr<Message>> messages;
    while (!queue_.empty()) {
      messages.push_back(queue_.front());
      queue_.pop_front();
    }
    return messages;
  }

  size_t size() {
    mtx_.lock();
    size_t size = queue_.size();
    mtx_.unlock();
    return size;
  }

 private:
  std::deque<std::shared_ptr<Message>> queue_;
  std::mutex mtx_;
  std::condition_variable not_empty_;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_CHANNEL_H_
