#include "pelton/sql/connections/encryption.h"

#include "glog/logging.h"

namespace pelton {
namespace sql {

#define MAX_LEN 10000

// a global random string that is the same for all keys and users.
unsigned char nonce[crypto_aead_aes256gcm_NPUBBYTES];

// initializes libsodium and sets nonce
void InitializeEncryption() {
  // Initialize libsodium.
  sodium_init();

  // set nonce
  randombytes_buf(nonce, sizeof nonce);
}

// generates random 256-bit key
unsigned char* KeyGen() {
  unsigned char *key = new unsigned char[crypto_aead_aes256gcm_KEYBYTES];
  crypto_aead_aes256gcm_keygen(key);
  return key;
}

// encrypts pt (plaintext) with key and returns ct (ciphertext)
std::string Encrypt(unsigned char *key, std::string pt) {
  size_t known_length = pt.size() + crypto_aead_aes256gcm_ABYTES;
  unsigned char *ct = new unsigned char[known_length];
  long long unsigned ct_len = 0;
  const char *pt_buffer = pt.c_str();
  auto casted_pt_buffer = reinterpret_cast<const unsigned char *>(pt_buffer);

  if (crypto_aead_aes256gcm_encrypt(ct, &ct_len,
                                casted_pt_buffer, pt.size(),
                                nullptr, 0, NULL, nonce, key) != 0) {
    LOG(FATAL) << "Cannot encrypt!";
  }

  std::string ct_str(reinterpret_cast<char const*>(ct), ct_len);
  delete[] ct;
  return ct_str;
}

// decrypts ct (ciphertext) with key and returns pt (plaintext)
std::string Decrypt(unsigned char *key, std::string ct) {
  unsigned char *pt = new unsigned char[MAX_LEN];
  long long unsigned pt_len = 0;
  const char *ct_buffer = ct.c_str();
  auto casted_ct_buffer = reinterpret_cast<const unsigned char *>(ct_buffer);

  if (ct.size() < crypto_aead_aes256gcm_ABYTES
      || crypto_aead_aes256gcm_decrypt(pt, &pt_len, NULL,
                                       casted_ct_buffer, ct.size(),
                                       nullptr, 0, nonce, key) != 0) {
    LOG(FATAL) << "Cannot decrypt!";
  }

  std::string pt_str(reinterpret_cast<char const*>(pt), pt_len);
  delete[] pt;
  return pt_str;
}

}  // namespace sql
}  // namespace pelton
