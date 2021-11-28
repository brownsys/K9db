// Defines the state of our sharding adapter.
//
// The state includes information about how the schema is sharded,
// as well as an in memory cache of user data, and which shards are
// already created.

#ifndef PELTON_SHARDS_UPGRADABLE_LOCK_H_
#define PELTON_SHARDS_UPGRADABLE_LOCK_H_

// NOLINTNEXTLINE
#include <mutex>
// NOLINTNEXTLINE
#include <shared_mutex>

namespace pelton {
namespace shards {

// Forward declaration for friends.
class UniqueLock;

// Upgradable Shared Mutex: somewhat mimics std::shared_mutex but with upgrade.
// This struct cannot be copied or moved, due to std::mutex mtx.
struct UpgradableMutex {
  // reader/writer locks to protect sharder state against concurrent
  // modification when there are multiple clients. We need two of them
  // to implement the safe upgrade protocol in Upgrade().
  std::shared_mutex shared1;
  std::shared_mutex shared2;
  // another mutex is needed so that we can "upgrade" the reader locks to
  // unique locks when it turns out that an operation needs to modify the
  // sharder metadata (e.g., updates and inserts). This lock excludes
  // other upgraders and avoids a race between upgraders.
  std::mutex mtx;
};

// A shared upgradable lock over UpgradableMutex: mimics std::shared_lock, but
// can be atomically upgraded to a unique lock!
// Class is movable but not copyable.
class SharedLock {
 public:
  // Shared lock from scratch.
  explicit SharedLock(UpgradableMutex *mutex);
  // Shared lock by downgrading from a UniqueLock.
  explicit SharedLock(UniqueLock &&unique);
  // Default destructor will unlock the locks!
 private:
  UpgradableMutex *mtx_;
  // Both locks must be readers!
  std::shared_lock<std::shared_mutex> r1_;
  std::shared_lock<std::shared_mutex> r2_;
  // UniqueLock needs access to internals to upgrade!
  friend UniqueLock;
};

// A unique lock over UpgradableMutex: mimics std::unique_lock.
// Class is movable but not copyable.
class UniqueLock {
 public:
  // Unique lock from scratch.
  explicit UniqueLock(UpgradableMutex *mutex);
  // Upgrade a shared lock to a unique lock!
  explicit UniqueLock(SharedLock &&shared);
  // Default destructor will unlock the locks!
 private:
  UpgradableMutex *mtx_;
  // Lock 1 may be held as a writer (initially a unique lock) or as a reader,
  // when a shared lock is upgraded.
  std::unique_lock<std::shared_mutex> w1_;
  std::shared_lock<std::shared_mutex> r1_;
  // Lock 2 must be a writer.
  std::unique_lock<std::shared_mutex> w2_;
  bool upgraded_;
  // SharedLock needs access to internals to downgrade!
  friend SharedLock;
};

}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_UPGRADABLE_LOCK_H_
