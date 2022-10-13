#ifndef PELTON_UTIL_SHARD_NAME_H_
#define PELTON_UTIL_SHARD_NAME_H_

#include <functional>
#include <memory>
#include <string>
// NOLINTNEXTLINE
#include <string_view>
// NOLINTNEXTLINE
#include <variant>

#include "rocksdb/slice.h"

namespace pelton {
namespace util {

class ShardName {
 public:
  // Result in owned shard name.
  ShardName(const std::string &shard_kind, const std::string &user_id);
  explicit ShardName(std::string &&shard_name);

  // REsult in an unowned slice (whose lifetime matches the slice).
  explicit ShardName(const rocksdb::Slice &shard_name);

  // O(1) if owned, string copy if not owned.
  std::string ByMove();

  // O(1) if owned, string copy if not owned.
  const std::string &ByRef() const;

  // O(1) in any case.
  std::string_view AsView() const;
  rocksdb::Slice AsSlice() const;
  std::string_view ShardKind() const;
  std::string_view UserID() const;

  // For use in hashing-based containers.
  size_t Hash() const;
  bool operator==(const ShardName &o) const;
  bool operator==(const std::string &o) const;
  bool operator==(const rocksdb::Slice &o) const;

 private:
  // The shard name may be owned or unowned.
  std::variant<std::string, rocksdb::Slice> shard_name_;

  // Split is the index that separates the shard kind form user id.
  // It's computed lazyily and cached here for future invocations.
  mutable int split_;

  // We want to support cheap reference access to the shard name for
  // compatibility with string-based API. This is trivial for when the shard
  // name is owned. When it is not owned, we make an owned copy here, and
  // return that ref in current and future invocations
  mutable std::unique_ptr<std::string> owned_str_;

  // Computes the split index (once).
  void FindSplit() const;
};

}  // namespace util
}  // namespace pelton

namespace std {

template <>
struct hash<pelton::util::ShardName> {
  size_t operator()(const pelton::util::ShardName &o) const { return o.Hash(); }
};

}  // namespace std

#endif  // PELTON_UTIL_SHARD_NAME_H_
