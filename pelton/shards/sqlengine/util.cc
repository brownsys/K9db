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
  if (HASHMAP.count(user_id) == 0) {
    char hex_buffer[MD5_DIGEST_LENGTH * 2 + 1];
    unsigned char md5_buffer[MD5_DIGEST_LENGTH];
    MD5(reinterpret_cast<const unsigned char *>(user_id.c_str()),
        user_id.size(), md5_buffer);
    for (size_t i = 0; i < MD5_DIGEST_LENGTH; i++) {
      // NOLINTNEXTLINE
      snprintf(hex_buffer + (i * 2), 3, "%02x", md5_buffer[i]);
    }
    // Drop some byes of the hash so that the name fit.
    // TODO(babman): handle this better.
    hex_buffer[24] = '\0';
    HASHMAP.insert({user_id, std::string(hex_buffer)});
    return absl::StrCat(shard_kind.substr(0, 1), hex_buffer);
  } else {
    return absl::StrCat(shard_kind.substr(0, 1), HASHMAP.at(user_id));
  }
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
