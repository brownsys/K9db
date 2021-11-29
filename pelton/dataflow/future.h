#ifndef PELTON_DATAFLOW_FUTURE_H_
#define PELTON_DATAFLOW_FUTURE_H_

#include <atomic>
#include <condition_variable>
#include <mutex>

namespace pelton {
namespace dataflow {

// Forward declare Promise
class Promise;

class Future {
 public:
  Future() : semaphore_(0) { this->counter_.store(0); }

  // Cannot copy a future.
  Future(const Future &other) = delete;
  Future &operator=(const Future &other) = delete;
  // Cannot move a future.
  Future(Future &&other) = delete;
  Future &operator=(Future &&other) = delete;

  // Get the initial promise form an existing future object.
  Promise GetPromise();
  void Increment();
  void Decrement();
  // Blocks until the future resolves.
  void Get();

  // Accessors
  const uint16_t counter() { return this->counter_.load(); }

 private:
  std::atomic<uint16_t> counter_;
  std::binary_semaphore semaphore_;
};

class Promise {
 public:
  // Cannot copy a promise.
  Promise(const Promise &other) = delete;
  Promise &operator=(const Promise &other) = delete;

  Promise(Promise &&other) {
    this->future_ = other.future_;
    other.future_ = nullptr;
  }
  Promise &operator=(Promise &&other) {
    if (this != &other) {
      this->future_ = other.future_;
      other.future_ = nullptr;
    }
    return *this;
  }

  void Set();
  Promise Derive();
  ~Promise();

 private:
  explicit Promise(Future *future) : future_(future) {
    // Update corresponding future if the promise is genuine.
    if (this->future_ != nullptr) {
      this->future_->Increment();
    }
  }
  Future *future_;

  // The initial promise can only be created via an existing future object.
  friend class Future;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_FUTURE_H_
