// An "Upgradable lock".
//
// This lock acts similar to std::shared_mutex, and can instantiate
// a unique lock (for excluding other reads and writers), and a shared lock
// (for excluding other writers).
//
// Additionally, the lock can be *conditionally* upgraded from a shared lock to
// a unique one.
// The upgrade is not entirely atomic: there is an opportunity for other writers
// -- that were initially excluded by the shared lock prior to upgrade --
// to unblock and execute between the time the upgrade is initialized and when
// it finally returns / finishes.
//
// As a result, upgrade takes in a lambda expressing a condition, and behaves
// as follows:
// 1. First, while mainitaing the current shared lock, it evaluates the
//    condition.
// 2. If the condition is false, no upgrade is needed.
// 3. If the condition is true, the shared lock is released and then a unique
//    lock is acquired.
// 4. Then the condition is evaluated again, if it becomes false, then another
//    writer have performed some work that affects the calling logic, and
//    the calling logic needs to address this.
// 5. If the condition remains true, then any other writers were irrelavent.
// 6. The new upgraded UniqueLock is returned, as well as the value of the
//    condition.

#ifndef K9DB_UTIL_UPGRADABLE_LOCK_H_
#define K9DB_UTIL_UPGRADABLE_LOCK_H_

#include <functional>
// NOLINTNEXTLINE
#include <mutex>
#include <optional>
// NOLINTNEXTLINE
#include <shared_mutex>
#include <utility>

namespace k9db {
namespace util {

// Forward declaration for friends.
class UniqueLock;

// Upgradable Shared Mutex: just a usual shared_mutex.
using UpgradableMutex = std::shared_mutex;

// A unique lock over UpgradableMutex: mimics std::unique_lock.
// Class is movable but not copyable.
class UniqueLock : public std::unique_lock<UpgradableMutex> {
 public:
  // Unique lock from scratch.
  explicit UniqueLock(UpgradableMutex *mutex);

 private:
  UpgradableMutex *mtx_;
};

// A shared upgradable lock over UpgradableMutex: mimics std::shared_lock, but
// can be atomically upgraded to a unique lock!
// Class is movable but not copyable.
class SharedLock : public std::shared_lock<UpgradableMutex> {
 public:
  // Shared lock from scratch.
  explicit SharedLock(UpgradableMutex *mutex);

  // Conditional upgrade.
  std::pair<std::optional<UniqueLock>, bool> UpgradeIf(
      std::function<bool(void)> &&cond);

 private:
  UpgradableMutex *mtx_;
};

}  // namespace util
}  // namespace k9db

#endif  // K9DB_UTIL_UPGRADABLE_LOCK_H_
