#ifndef K9DB_DATAFLOW_FUTURE_H_
#define K9DB_DATAFLOW_FUTURE_H_

#include <atomic>
// NOLINTNEXTLINE
#include <condition_variable>
// NOLINTNEXTLINE
#include <mutex>

#include "gtest/gtest_prod.h"

namespace k9db {
namespace dataflow {

// Forward declare Promise
class Promise;

class Future {
 public:
  explicit Future(bool consistent) : consistent_(consistent), semaphore_(0) {
    if (consistent) {
      this->counter_.store(1);
    }
  }

  // Cannot copy a future.
  Future(const Future &other) = delete;
  Future &operator=(const Future &other) = delete;
  // Cannot move a future.
  Future(Future &&other) = delete;
  Future &operator=(Future &&other) = delete;

  // Get the initial promise form an existing future object.
  Promise GetPromise();

  // Blocks until the future resolves.
  void Wait();

 private:
  void Increment();
  void Decrement();

  bool consistent_;
  std::atomic<uint16_t> counter_;
  std::binary_semaphore semaphore_;

  // Allow promise to change counter value.
  friend class Promise;
  FRIEND_TEST(FutureTest, BasicTest);
  FRIEND_TEST(FutureTest, InconsistentTest);
};

class Promise {
 public:
  // Cannot copy a promise.
  Promise(const Promise &other) = delete;
  Promise &operator=(const Promise &other) = delete;

  // Promises can be moved.
  Promise(Promise &&other) : future_(other.future_) { other.future_ = nullptr; }
  Promise &operator=(Promise &&other) {
    if (this != &other) {
      this->future_ = other.future_;
      other.future_ = nullptr;
    }
    return *this;
  }

  // Destructor decrements counter of corresponding future.
  ~Promise();

  // Resolve the promise.
  void Resolve();

  // Create a derived promise (increments the future counter).
  Promise Derive() const;

  // This is used in micro-tests and micro-benchmarks
  static Promise None;

 private:
  // Cannot be constructed publicly. Must be done from the future.
  explicit Promise(Future *future) : future_(future) {
    // Update corresponding future if the promise is genuine.
    if (this->future_ != nullptr) {
      this->future_->Increment();
    }
  }

  // The corresponding future.
  // if nullptr then nothing to resolve/set.
  Future *future_;

  // The initial promise can only be created via an existing future object.
  friend class Future;
  FRIEND_TEST(FutureTest, BasicTest);
  FRIEND_TEST(FutureTest, InconsistentTest);
};

}  // namespace dataflow
}  // namespace k9db

#endif  // K9DB_DATAFLOW_FUTURE_H_
