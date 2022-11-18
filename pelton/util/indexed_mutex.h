// A mutex with a unique index. We use this to acquire several mutex in a
// strict order.
#ifndef PELTON_UTIL_INDEXED_MUTEX_H_
#define PELTON_UTIL_INDEXED_MUTEX_H_

// NOLINTNEXTLINE
#include <mutex>
// NOLINTNEXTLINE
#include <shared_mutex>

namespace pelton {
namespace util {

// A lock that remembers the index of its mutex.
template <typename L>
class IndexedLock {
 public:
  IndexedLock(size_t idx, std::shared_mutex *mtx) : idx_(idx), lock_(*mtx) {}
  size_t Index() const { return this->idx_; }

 private:
  size_t idx_;
  L lock_;
};

// Instantiate for unique and shared locks.
using IndexedUniqueLock = IndexedLock<std::unique_lock<std::shared_mutex>>;
using IndexedSharedLock = IndexedLock<std::shared_lock<std::shared_mutex>>;

// A mutex with an index.
// Comparisons are done on the index to ensure a strict order.
class IndexedMutex {
 public:
  using BaseMutex = std::shared_mutex;

  explicit IndexedMutex(size_t idx, BaseMutex *mtx) : idx_(idx), mtx_(mtx) {}
  size_t Index() const { return this->idx_; }

  // Comparison based on index (for order).
  bool operator<(const IndexedMutex &o) const;
  bool operator==(const IndexedMutex &o) const;

  // Acquire this lock.
  IndexedUniqueLock LockUnique() const;
  IndexedSharedLock LockShared() const;

 private:
  size_t idx_;
  mutable BaseMutex *mtx_;
};

}  // namespace util
}  // namespace pelton

#endif  // PELTON_UTIL_INDEXED_MUTEX_H_
