#include "k9db/util/upgradable_lock.h"

namespace k9db {
namespace util {

// UniqueLock.
UniqueLock::UniqueLock(UpgradableMutex *mtx)
    : std::unique_lock<UpgradableMutex>(*mtx), mtx_(mtx) {}

// SharedLock.
SharedLock::SharedLock(UpgradableMutex *mtx)
    : std::shared_lock<UpgradableMutex>(*mtx), mtx_(mtx) {}

// Conditional upgrade.
std::pair<std::optional<UniqueLock>, bool> SharedLock::UpgradeIf(
    std::function<bool(void)> &&cond) {
  // First check if upgrade is needed to begin with.
  if (!cond()) {
    return std::pair(std::optional<UniqueLock>(), false);
  }

  // Need to upgrade.
  this->unlock();
  UniqueLock upgraded(this->mtx_);
  this->mtx_ = nullptr;

  // Check condition again.
  return std::make_pair(std::move(upgraded), cond());
}

}  // namespace util
}  // namespace k9db
