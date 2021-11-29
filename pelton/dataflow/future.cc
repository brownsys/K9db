#include "pelton/dataflow/future.h"

#include "glog/logging.h"

namespace pelton {
namespace dataflow {

Promise Future::GetPromise() { return Promise(this); }

void Future::Increment() { this->counter_++; }

void Future::Decrement() {
  uint16_t updated_value = --this->counter_;
  if (updated_value == 0) {
    // Only one thread should be waiting on the single future object.
    this->semaphore_.release();
  }
}

void Future::Get() {
  // Only wait on the semaphore if there are pending promises.
  if (this->counter_.load() > 0) {
    this->semaphore_.acquire();
  }
}

void Promise::Set() {
  this->future_->Decrement();
  this->future_ = nullptr;
}

Promise Promise::Derive() { return Promise(this->future_); }

Promise::~Promise() {
  if (this->future_ != nullptr) {
    LOG(INFO) << "Future counter: " << this->future_->counter();
    LOG(FATAL) << "Broken promise";
  }
}

}  // namespace dataflow
}  // namespace pelton
