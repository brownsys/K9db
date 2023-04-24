#include "k9db/dataflow/future.h"

#include "glog/logging.h"

namespace k9db {
namespace dataflow {

Promise Future::GetPromise() {
  return Promise(this->consistent_ ? this : nullptr);
}

void Future::Increment() {
  uint16_t value = this->counter_++;
  CHECK_GT(value, 0) << "Late promise";
}

void Future::Decrement() {
  uint16_t value = this->counter_--;
  CHECK_GT(value, 0) << "Early promise";
  if (value == 1) {
    // Only one thread should be waiting on the single future object.
    this->semaphore_.release();
  }
}

void Future::Wait() {
  if (this->consistent_) {
    this->Decrement();
    // Only wait on the semaphore if there are pending promises.
    if (this->counter_.load() > 0) {
      this->semaphore_.acquire();
    }
  }
}

void Promise::Resolve() {
  if (this->future_ != nullptr) {
    this->future_->Decrement();
    this->future_ = nullptr;
  }
}

Promise Promise::Derive() const { return Promise(this->future_); }

Promise::~Promise() {
  if (this->future_ != nullptr) {
    LOG(FATAL) << "Broken promise";
  }
}

Promise Promise::None = Promise(nullptr);

}  // namespace dataflow
}  // namespace k9db
