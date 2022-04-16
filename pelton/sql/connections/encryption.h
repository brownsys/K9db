#ifndef PELTON_SQL_CONNECTIONS_ENCRYPTION_H__
#define PELTON_SQL_CONNECTIONS_ENCRYPTION_H__

#include <iostream>
#include <string>
#include <utility>
#include <chrono>
#include <assert.h>

#include "pelton/sql/connections/rocksdb_util.h"
#include "sodium.h"

#define PELTON_ENCRYPTION

namespace pelton {
namespace sql {

// initializes libsodium and sets nonce
void InitializeEncryption();

// generates random 256-bit key
unsigned char* KeyGen();

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

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_CONNECTIONS_ROCKSDB_UTIL_H__

