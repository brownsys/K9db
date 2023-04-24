#include "k9db/util/shard_name.h"

#include <utility>

#include "glog/logging.h"

namespace k9db {
namespace util {

// Helpeful macros.
#define HOLDS_STRING() std::holds_alternative<std::string>(this->shard_name_)
#define GET_STRING() std::get<std::string>(this->shard_name_)
#define GET_SLICE() std::get<rocksdb::Slice>(this->shard_name_)
#define EXTRACT(expr) \
  std::visit([&](const auto &s) { return expr; }, this->shard_name_)

// Owned shard name.
ShardName::ShardName(const std::string_view &shard_kind,
                     const std::string_view &user_id)
    : shard_name_(std::string("")), split_(shard_kind.size()) {
  std::string &shard_name = std::get<std::string>(this->shard_name_);
  shard_name.reserve(shard_kind.size() + 2 + user_id.size());
  shard_name.append(shard_kind);
  shard_name.push_back('_');
  shard_name.push_back('_');
  shard_name.append(user_id);
}

ShardName::ShardName(std::string &&shard_name)
    : shard_name_(std::move(shard_name)), split_(-1) {}

// Unowned slices.
ShardName::ShardName(const rocksdb::Slice &shard_name)
    : shard_name_(shard_name), split_(-1) {}

/*
 * Accessors for components.
 */

// O(1) if owned, string copy if not owned.
std::string ShardName::ByMove() {
  std::string result;
  if (HOLDS_STRING()) {
    result = std::move(GET_STRING());
  } else {
    result = GET_SLICE().ToString();
  }
  return result;
}
const std::string &ShardName::ByRef() const {
  if (HOLDS_STRING()) {
    return GET_STRING();
  } else if (this->owned_str_ == nullptr) {
    const rocksdb::Slice &slice = GET_SLICE();
    this->owned_str_ = std::make_unique<std::string>(slice.ToString());
  }
  return *this->owned_str_;
}

// O(1) in any case.
std::string_view ShardName::AsView() const {
  return EXTRACT(std::string_view(s.data(), s.size()));
}
rocksdb::Slice ShardName::AsSlice() const {
  return EXTRACT(rocksdb::Slice(s.data(), s.size()));
}

std::string_view ShardName::ShardKind() const {
  this->FindSplit();
  return EXTRACT(std::string_view(s.data(), this->split_));
}

std::string_view ShardName::UserID() const {
  this->FindSplit();
  return EXTRACT(std::string_view(s.data() + this->split_ + 2,
                                  s.size() - this->split_ - 2));
}

// For use in hashing-based containers.
size_t ShardName::Hash() const {
  return std::hash<std::string_view>()(this->AsView());
}
bool ShardName::operator==(const ShardName &o) const {
  return this->AsView() == o.AsView();
}
bool ShardName::operator==(const rocksdb::Slice &o) const {
  return this->AsSlice() == o;
}
bool ShardName::operator==(const std::string &o) const {
  return this->AsView() == o;
}

// Finding the split.
void ShardName::FindSplit() const {
  if (this->split_ < 0) {
    const char *data = EXTRACT(s.data());
    int size = static_cast<int>(EXTRACT(s.size()));
    for (this->split_ = 0; this->split_ + 1 < size; this->split_++) {
      if (data[this->split_] == '_' && data[this->split_ + 1] == '_') {
        return;
      }
    }
    LOG(FATAL) << "Cannot split shardname '" << this->AsView() << "'";
  }
}

}  // namespace util
}  // namespace k9db
