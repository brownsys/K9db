#ifndef PELTON_SQL_ROCKSDB_ENCRYPTION_H_
#define PELTON_SQL_ROCKSDB_ENCRYPTION_H_

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "pelton/sql/rocksdb/encode.h"
#include "pelton/sql/rocksdb/metadata.h"
#include "pelton/util/shard_name.h"
#include "pelton/util/upgradable_lock.h"
#include "rocksdb/comparator.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"

namespace pelton {
namespace sql {
namespace rocks {

// Forward declaration for use in Cipher.
// class EncryptionManager;

// Our Cipher text type.
// Basically a wrapper around std::string.
// Can only be constructed by EncryptionManager.
class Cipher {
 public:
  Cipher() = delete;

  rocksdb::Slice Data() const { return rocksdb::Slice(this->cipher_); }
  std::string &&Release() { return std::move(this->cipher_); }

  bool operator==(const Cipher &o) const { return this->cipher_ == o.cipher_; }
  bool operator!=(const Cipher &o) const { return this->cipher_ != o.cipher_; }

  static Cipher FromDB(std::string &&cipher) {
    return Cipher(std::move(cipher));
  }

 private:
  std::string cipher_;

  // Can only be called by friends.
  explicit Cipher(std::string &&cipher) : cipher_(std::move(cipher)) {}

  // EncryptionManager can construct this.
  friend class EncryptionManager;
  friend class std::hash<Cipher>;
};

// Encrypted keys are encrypted piece-wise.
// The shard is encrypted separately from PK value.
// Their format is <shard_name><pk_value><len of shard: 2 bytes (unsigned)>.
// This allows encrypted lookup by shard_name and pk_value.
// If encryption is off, this basically becomes a wrapper around a regular
// unencrypted RocksdbSequence.
class EncryptedKey {
 public:
  using Offset = uint16_t;

  // Reading from rocksdb.
  explicit EncryptedKey(std::string &&data) : data_(std::move(data)) {}
  explicit EncryptedKey(const rocksdb::Slice &data)
      : data_(data.data(), data.size()) {}

  // Constructed after encrypting.
  EncryptedKey(Cipher &&shard_cipher, const Cipher &pk_cipher);

  // Accessors.
  rocksdb::Slice Data() const { return rocksdb::Slice(this->data_); }
  std::string &&Release() { return std::move(this->data_); }

  // Equality is over underlying string.
  bool operator==(const EncryptedKey &o) const {
    return this->data_ == o.data_;
  }
  bool operator!=(const EncryptedKey &o) const {
    return this->data_ != o.data_;
  }

  // For reading/decoding.
  rocksdb::Slice GetShard() const;
  rocksdb::Slice GetPK() const;

  static Offset ShardSize(const rocksdb::Slice &slice);

 private:
  std::string data_;

  friend class std::hash<EncryptedKey>;
};

// An encrypted value/row is just a blackbox cipher with no structure.
using EncryptedValue = Cipher;
using EncryptedPrefix = Cipher;

// Encryption manager is responsible for encrypting / decrypting.
// The functions of encryption manager basically become noops if encryption
// is turned off.
class EncryptionManager {
 public:
  EncryptionManager();
  void Initialize(RocksdbMetadata *metadata);

  // Encryption of keys and values of records.
  EncryptedKey EncryptKey(RocksdbSequence &&k) const;
  EncryptedValue EncryptValue(const std::string &shard_name,
                              RocksdbSequence &&v);

  // Decryption of records.
  RocksdbSequence DecryptKey(EncryptedKey &&k) const;
  RocksdbSequence DecryptValue(const std::string &shard_name,
                               EncryptedValue &&v) const;

  // Encrypts a key for use with rocksdb Seek.
  // Given our PeltonPrefixTransform, the seek prefix is the shard name.
  // (Must be passed to this function without a trailing __ROCKSSEP).
  EncryptedPrefix EncryptSeek(util::ShardName &&seek_key) const;

 private:
  std::unique_ptr<unsigned char[]> global_key_;
  std::unique_ptr<unsigned char[]> global_nonce_;
  std::unordered_map<std::string, std::unique_ptr<unsigned char[]>> keys_;
  mutable util::UpgradableMutex mtx_;
  RocksdbMetadata *metadata_;

  // Get the key of the given user, create the key for that user if it does not
  // exist.
  const unsigned char *GetOrCreateUserKey(const std::string &shard_name);
  const unsigned char *GetUserKey(const std::string &shard_name) const;
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

}  // namespace rocks
}  // namespace sql
}  // namespace pelton

// Overlead hash function for encrypted stuff.
namespace std {

template <>
struct hash<pelton::sql::rocks::Cipher> {
  size_t operator()(const pelton::sql::rocks::Cipher &o) const {
    return hash<std::string>{}(o.cipher_);
  }
};

template <>
struct hash<pelton::sql::rocks::EncryptedKey> {
  size_t operator()(const pelton::sql::rocks::EncryptedKey &o) const {
    return hash<std::string>{}(o.data_);
  }
};

}  // namespace std

#endif  // PELTON_SQL_ROCKSDB_ENCRYPTION_H_
