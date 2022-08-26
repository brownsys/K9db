#include "pelton/sql/rocksdb/encryption.h"

#ifndef PELTON_ENCRYPTION

namespace pelton {
namespace sql {

// EncryptedRecord.
rocksdb::Slice EncryptedRecord::GetShard() const {
  return ExtractColumn(this->key_, 0);
}
rocksdb::Slice EncryptedRecord::GetPK() const {
  return ExtractColumn(this->key_, 1);
}

// EncryptionManager.
// Construct encryption manager.
EncryptionManager::EncryptionManager() = default;

// Encrypts a record and returns the encrypt key and value.
EncryptedRecord EncryptionManager::EncryptRecord(const std::string &user_id,
                                                 RocksdbRecord &&r) {
  return EncryptedRecord(r.ReleaseKey(), r.ReleaseValue());
}

// Encrypts a fully encoded key with a shardname and a pk.
rocksdb::Slice EncryptionManager::EncryptKey(rocksdb::Slice key) { return key; }

// Decrypts an encrypted record (both key and value).
RocksdbRecord EncryptionManager::DecryptRecord(const std::string &user_id,
                                               EncryptedRecord &&r) const {
  return RocksdbRecord(r.ReleaseKey(), r.ReleaseValue());
}

// Only decrypts the key from this given record.
std::string EncryptionManager::DecryptKey(std::string &&key) const {
  return std::move(key);
}

// encrypts user_id and id components of seek.
std::string EncryptionManager::EncryptSeek(std::string &&seek_key) const {
  seek_key.push_back(__ROCKSSEP);
  return std::move(seek_key);
}

// PeltonPrefixTransform.
rocksdb::Slice PeltonPrefixTransform::Transform(const rocksdb::Slice &k) const {
  return ExtractColumn(k, 0);
}

// Plain bytewise comparator.
const rocksdb::Comparator *PeltonComparator() {
  return rocksdb::BytewiseComparator();
}

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_ENCRYPTION
