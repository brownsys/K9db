// This file contains utility functions, mostly for standarizing
// name (suffixes etc) of things.

#include "pelton/shards/sqlengine/util.h"

#include <openssl/md5.h>

#include <cstdio>
#include <unordered_map>

namespace pelton {
namespace shards {
namespace sqlengine {

static std::unordered_map<std::string, std::string> HASHMAP;

std::string NameShard(const ShardKind &shard_kind, const UserId &user_id) {
  return shard_kind + user_id;
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
