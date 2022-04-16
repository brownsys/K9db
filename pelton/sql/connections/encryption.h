#ifndef PELTON_SQL_CONNECTIONS_ENCRYPTION_H__
#define PELTON_SQL_CONNECTIONS_ENCRYPTION_H__

#include <iostream>
#include <string>
#include <utility>
#include <chrono>
#include <assert.h>

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

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_CONNECTIONS_ROCKSDB_UTIL_H__

