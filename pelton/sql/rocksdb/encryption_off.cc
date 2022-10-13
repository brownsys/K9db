// clang-format off
// NOLINTNEXTLINE
#include "pelton/sql/rocksdb/encryption.h"
// clang-format on

#include "glog/logging.h"

namespace pelton {
namespace sql {

/*
 * EncryptedKey
 */
EncryptedKey::EncryptedKey(Cipher &&shard_cipher, const Cipher &pk_cipher) {
  LOG(FATAL) << "ENCRYPTION OFF!";
}
rocksdb::Slice EncryptedKey::GetShard() const {
  return ExtractColumn(this->data_, 0);
}
rocksdb::Slice EncryptedKey::GetPK() const {
  return ExtractColumn(this->data_, 1);
}
EncryptedKey::Offset EncryptedKey::ShardSize(const rocksdb::Slice &slice) {
  LOG(FATAL) << "ENCRYPTION OFF!";
}

/*
 * EncryptionManager
 */

// Construct encryption manager.
EncryptionManager::EncryptionManager() = default;

// Encryption of keys and values of records.
EncryptedKey EncryptionManager::EncryptKey(RocksdbSequence &&k) const {
  return EncryptedKey(k.Release());
}

EncryptedValue EncryptionManager::EncryptValue(const std::string &shard_name,
                                               RocksdbSequence &&v) {
  return EncryptedValue(v.Release());
}

// Decryption of records.
RocksdbSequence EncryptionManager::DecryptKey(EncryptedKey &&k) const {
  return RocksdbSequence(k.Release());
}

RocksdbSequence EncryptionManager::DecryptValue(const std::string &shard_name,
                                                EncryptedValue &&v) const {
  return RocksdbSequence(v.Release());
}

// Encrypts a key for use with rocksdb Seek.
EncryptedPrefix EncryptionManager::EncryptSeek(util::ShardName &&seek) const {
  std::string seek_key = seek.ByMove();
  seek_key.push_back(__ROCKSSEP);
  return Cipher(std::move(seek_key));
}

// Helpers are unused.
const unsigned char *EncryptionManager::GetOrCreateUserKey(
    const std::string &shard_name) {
  LOG(FATAL) << "ENCRYPTION OFF!";
  return nullptr;
}
const unsigned char *EncryptionManager::GetUserKey(
    const std::string &shard_name) const {
  LOG(FATAL) << "ENCRYPTION OFF!";
  return nullptr;
}

/*
 * PeltonPrefixTransform
 */
rocksdb::Slice PeltonPrefixTransform::Transform(const rocksdb::Slice &k) const {
  return ExtractColumn(k, 0);
}

/*
 * PeltonComparator
 */
const rocksdb::Comparator *PeltonComparator() {
  return rocksdb::BytewiseComparator();
}

}  // namespace sql
}  // namespace pelton
