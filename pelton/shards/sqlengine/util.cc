// This file contains utility functions, mostly for standarizing
// name (suffixes etc) of things.

#include "pelton/shards/sqlengine/util.h"

#include <openssl/md5.h>

#include <cstdio>

namespace pelton {
namespace shards {
namespace sqlengine {

std::string NameShard(const ShardKind &shard_kind, const UserId &user_id) {
  char hex_buffer[MD5_DIGEST_LENGTH * 2 + 1];
  unsigned char md5_buffer[MD5_DIGEST_LENGTH];
  MD5(reinterpret_cast<const unsigned char *>(user_id.c_str()), user_id.size(),
      md5_buffer);
  for (size_t i = 0; i < MD5_DIGEST_LENGTH; i++) {
    // NOLINTNEXTLINE
    snprintf(hex_buffer + (i * 2), 3, "%02x", md5_buffer[i]);
  }
  return absl::StrCat(shard_kind.substr(0, 1), hex_buffer);
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
