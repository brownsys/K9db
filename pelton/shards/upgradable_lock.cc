// Defines the state of our sharding adapter.
//
// The state includes information about how the schema is sharded,
// as well as an in memory cache of user data, and which shards are
// already created.

#include "pelton/shards/upgradable_lock.h"

#include <utility>

#include "glog/logging.h"

namespace pelton {
namespace shards {

// SharedLock.
SharedLock::SharedLock(UpgradableMutex *mutex)
    : mtx_(mutex), r1_(mutex->shared1), r2_(mutex->shared2) {}

SharedLock::SharedLock(UniqueLock &&unique) : mtx_(unique.mtx_) {
  CHECK(unique.upgraded_);
  // TODO(babman): this lock does not seem useful and introduces a deadlock.
  // std::unique_lock<std::mutex> lock(this->mtx_->mtx);
  this->r1_ = std::move(unique.r1_);
  unique.w2_.unlock();
  this->r2_ = std::shared_lock<std::shared_mutex>(this->mtx_->shared2);
}

// UniqueLock.
UniqueLock::UniqueLock(UpgradableMutex *mutex)
    : mtx_(mutex), w1_(mutex->shared1), w2_(mutex->shared2), upgraded_(false) {}

UniqueLock::UniqueLock(SharedLock &&shared)
    : mtx_(shared.mtx_), w1_(), upgraded_(true) {
  // TODO(babman): this lock does not seem useful and introduces a deadlock.
  // std::unique_lock<std::mutex> lock(this->mtx_->mtx);
  shared.r2_.unlock();
  this->r1_ = std::move(shared.r1_);
  this->w2_ = std::unique_lock<std::shared_mutex>(this->mtx_->shared2);
}

}  // namespace shards
}  // namespace pelton
