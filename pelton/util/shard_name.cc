#include "pelton/util/shard_name.h"

#include <utility>

#include "glog/logging.h"

namespace pelton {
namespace util {

ShardName::ShardName(const std::string &shard_kind, const std::string &user_id)
    : shard_name_(shard_kind + "__" + user_id), split_(shard_kind.size()) {}

ShardName::ShardName(const std::string &shard_name)
    : shard_name_(shard_name), split_(0) {
  for (; this->split_ < this->shard_name_.size() - 1; this->split_++) {
    if (this->shard_name_.at(this->split_) == '_' &&
        this->shard_name_.at(this->split_ + 1) == '_') {
      return;
    }
  }
  LOG(FATAL) << "Cannot split shardname '" << this->shard_name_ << "'";
}

const std::string &ShardName::String() const { return this->shard_name_; }
std::string &&ShardName::Release() { return std::move(this->shard_name_); }

std::string_view ShardName::ShardKind() const {
  return std::string_view(this->shard_name_.data(), this->split_);
}

std::string_view ShardName::UserID() const {
  return std::string_view(this->shard_name_.data() + this->split_ + 2,
                          this->shard_name_.size() - this->split_ - 2);
}

// For use in hashing-based containers.
size_t ShardName::Hash() const {
  return std::hash<std::string>()(this->shard_name_);
}
bool ShardName::operator==(const ShardName &o) const {
  return this->shard_name_ == o.shard_name_;
}
bool ShardName::operator==(const rocksdb::Slice &o) const {
  return this->shard_name_ == o;
}

}  // namespace util
}  // namespace pelton
