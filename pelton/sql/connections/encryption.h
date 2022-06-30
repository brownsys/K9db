#ifndef PELTON_SQL_CONNECTIONS_ENCRYPTION_H_
#define PELTON_SQL_CONNECTIONS_ENCRYPTION_H_

#include <iostream>
#include <string>
#include <utility>

#include "rocksdb/comparator.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"

#define PELTON_ENCRYPTION

namespace pelton {
namespace sql {

// initializes libsodium and sets nonce
void InitializeEncryption();

// generates random 256-bit key
unsigned char *KeyGen();

// encrypts pt (plaintext) with key and returns ct (ciphertext)
std::string Encrypt(unsigned char *key, std::string pt);

// decrypts ct (ciphertext) with key and returns pt (plaintext)
std::string Decrypt(unsigned char *key, std::string ct);

// encrypts user_id and id components of keys
std::string EncryptKey(unsigned char *key, std::string pt);

// decrypts user_id and id components of keys
std::string DecryptKey(unsigned char *key, std::string ct);

// encrypts user_id and id components of seek
std::string EncryptSeek(unsigned char *key, std::string pt);

// Encrypted prefix transform
class EncryptedPrefixTransform : public rocksdb::SliceTransform {
 public:
  EncryptedPrefixTransform() = default;

  const char *Name() const override { return "EncryptedPrefix"; }
  bool InDomain(const rocksdb::Slice &key) const override { return true; }
  rocksdb::Slice Transform(const rocksdb::Slice &key) const override;

 private:
  size_t seps_;
};

// Encrypted compartor
class EncryptedComparator : public rocksdb::Comparator {
 public:
  // Three-way comparison.  Returns value:
  //   < 0 iff "a" < "b",
  //   == 0 iff "a" == "b",
  //   > 0 iff "a" > "b"
  // Note that Compare(a, b) also compares timestamp if timestamp size is
  // non-zero. For the same user key with different timestamps, larger (newer)
  // timestamp comes first.
  int Compare(const rocksdb::Slice &a, const rocksdb::Slice &b) const override;
  const char *Name() const override { return "EncryptedComparator"; }

  // Advanced functions: these are used to reduce the space requirements
  // for internal data structures like index blocks.
  void FindShortestSeparator(std::string *start,
                             const rocksdb::Slice &limit) const override {}
  void FindShortSuccessor(std::string *key) const override {}

  // A singleton comparator.
  static const rocksdb::Comparator *Instance();

 private:
  const rocksdb::Comparator *bytewise_ = rocksdb::BytewiseComparator();
};

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_CONNECTIONS_ENCRYPTION_H_
