// A mutex with a unique index. We use this to acquire several mutex in a
// strict order.
#include "pelton/util/indexed_mutex.h"

namespace pelton {
namespace util {

// IndexedMutex.
bool IndexedMutex::operator<(const IndexedMutex &o) const {
  return this->idx_ < o.idx_;
}
bool IndexedMutex::operator==(const IndexedMutex &o) const {
  return this->idx_ == o.idx_;
}
IndexedUniqueLock IndexedMutex::LockUnique() const {
  return IndexedUniqueLock(this->idx_, this->mtx_);
}
IndexedSharedLock IndexedMutex::LockShared() const {
  return IndexedSharedLock(this->idx_, this->mtx_);
}

}  // namespace util
}  // namespace pelton
