#ifndef PELTON_SQL_ROCKSDB_ENCRYPTION_H_
#define PELTON_SQL_ROCKSDB_ENCRYPTION_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "pelton/shards/upgradable_lock.h"
#include "pelton/sql/rocksdb/encode.h"
#include "rocksdb/comparator.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"

// #define PELTON_ENCRYPTION

namespace pelton {
namespace sql {

class EncryptedRecord {
 public:
  // Only a key.
  explicit EncryptedRecord(const rocksdb::Slice &key)
      : key_(key.data(), key.size()) {}

  explicit EncryptedRecord(std::string &&key) : key_(std::move(key)) {}

  // Both key and value.
  EncryptedRecord(const rocksdb::Slice &key, const rocksdb::Slice &value)
      : key_(key.data(), key.size()), value_(value.data(), value.size()) {}

  EncryptedRecord(std::string &&key, std::string &&value)
      : key_(std::move(key)), value_(std::move(value)) {}

  // Accessors.
  rocksdb::Slice Key() const { return this->key_; }
  rocksdb::Slice Value() const { return this->value_; }
  std::string &&ReleaseKey() { return std::move(this->key_); }
  std::string &&ReleaseValue() { return std::move(this->value_); }

  // For reading/decoding.
  rocksdb::Slice GetShard() const;
  rocksdb::Slice GetPK() const;

 private:
  std::string key_;
  std::string value_;
};

class EncryptionManager {
 public:
  EncryptionManager();

  // Encrypts the key and value of a record.
  EncryptedRecord EncryptRecord(const std::string &user_id, RocksdbRecord &&r);

  // Decrypts an encrypted record (both key and value).
  RocksdbRecord DecryptRecord(const std::string &user_id,
                              EncryptedRecord &&r) const;

  // Encrypts a fully encoded key with a shardname and a pk.
#ifdef PELTON_ENCRYPTION
  std::string EncryptKey(rocksdb::Slice key);
#else
  rocksdb::Slice EncryptKey(rocksdb::Slice key);
#endif  // PELTON_ENCRYPTION

  // Only decrypts the key from this given record.
  std::string DecryptKey(std::string &&key) const;

  // Encrypts a key for use with rocksdb Seek.
  // Given our PeltonPrefixTransform, the seek prefix is the shard name.
  // (Must be passed to this function without a trailing __ROCKSSEP).
  std::string EncryptSeek(std::string &&seek_key) const;

 private:
#ifdef PELTON_ENCRYPTION
  std::unique_ptr<unsigned char[]> global_key_;
  std::unique_ptr<unsigned char[]> global_nonce_;
  std::unordered_map<std::string, std::unique_ptr<unsigned char[]>> keys_;
  mutable shards::UpgradableMutex mtx_;

  // Get the key of the given user, create the key for that user if it does not
  // exist.
  const unsigned char *GetOrCreateUserKey(const std::string &user_id);
  const unsigned char *GetUserKey(const std::string &user_id) const;
#endif  // PELTON_ENCRYPTION
};

// Extract the shard prefix from keys according to our rocksdb key format.
// Either encrypted or plain.
class PeltonPrefixTransform : public rocksdb::SliceTransform {
 public:
  PeltonPrefixTransform() = default;

  const char *Name() const override { return "PeltonPrefix"; }
  bool InDomain(const rocksdb::Slice &key) const override { return true; }

  rocksdb::Slice Transform(const rocksdb::Slice &key) const override;

 private:
  size_t seps_;
};

// Comparator for our rocksdb key format.
// Either encrypted or plain bytes.
const rocksdb::Comparator *PeltonComparator();

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_ROCKSDB_ENCRYPTION_H_
