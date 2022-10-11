#ifndef PELTON_UTIL_SHARD_NAME_H_
#define PELTON_UTIL_SHARD_NAME_H_

#include <functional>
#include <string>
// NOLINTNEXTLINE
#include <string_view>

#include "rocksdb/slice.h"

namespace pelton {
namespace util {

class ShardName {
 public:
  ShardName(const std::string &shard_kind, const std::string &user_id);
  explicit ShardName(const std::string &shard_name);

  const std::string &String() const;
  std::string &&Release();
  std::string_view ShardKind() const;
  std::string_view UserID() const;

  // For use in hashing-based containers.
  size_t Hash() const;
  bool operator==(const ShardName &o) const;
  bool operator==(const rocksdb::Slice &o) const;

 private:
  std::string shard_name_;
  size_t split_;
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
