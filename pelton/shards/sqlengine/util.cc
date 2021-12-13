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

std::string Dequote(const std::string &st) {
  std::string s(st);
  s.erase(remove(s.begin(), s.end(), '\"'), s.end());
  s.erase(remove(s.begin(), s.end(), '\''), s.end());
  return s;
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
