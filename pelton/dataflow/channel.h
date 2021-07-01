#ifndef PELTON_DATAFLOW_CHANNEL_H_
#define PELTON_DATAFLOW_CHANNEL_H_

#include <deque>
#include <memory>
#include <vector>
#include <mutex>
#include <condition_variable>

#include "pelton/dataflow/batch_message.h"

namespace pelton{
namespace dataflow{

class Channel {
 public:
  bool Send(std::shared_ptr<BatchMessage> message) {
    mtx_.lock();
    queue_.push_back(message);
    mtx_.unlock();
    not_empty_.notify_all();
    return true;
  }

  // The queue gets flushed since we are following a multiple producer-single
  // consumer pattern
  std::vector<std::shared_ptr<BatchMessage>> Recv() {
    std::unique_lock<std::mutex> lock(mtx_);
    while(queue_.size()==0){
      not_empty_.wait(lock);
    }
    std::vector<std::shared_ptr<BatchMessage>> messages;
    while (!queue_.empty()) {
      messages.push_back(queue_.front());
      queue_.pop_front();
    }
    return messages;
  }

  size_t size(){
    mtx_.lock();
    size_t size = queue_.size();
    mtx_.unlock();
    return size;
  }

 private:
  // May consider using generics, do not need it as of now.
  std::deque<std::shared_ptr<BatchMessage>> queue_;
  std::mutex mtx_;
  std::condition_variable not_empty_;

};

} // namespace dataflow
} // namespace pelton

#endif  // PELTON_DATAFLOW_CHANNEL_H_
