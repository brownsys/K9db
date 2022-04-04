#include <iostream>
#include <string>

// NOLINTNEXTLINE
#include "sodium.h"

#define MAX_LEN 1024

// a global random string that is the same for all keys and users.
unsigned char nonce[crypto_aead_aes256gcm_NPUBBYTES];

// generates random 256-bit key
unsigned char* KeyGen() {
  static unsigned char key[crypto_aead_aes256gcm_KEYBYTES];
  crypto_aead_aes256gcm_keygen(key);
  return key;
}

// encrypts pt (plaintext) with key and returns ct (ciphertext)
std::string Encrypt(unsigned char *key, std::string pt) {
  size_t known_length = pt.size() + crypto_aead_aes256gcm_ABYTES;
  static unsigned char *ct = new unsigned char[known_length];
  long long unsigned ct_len = 0;
  const char *pt_buffer = pt.c_str();
  auto casted_pt_buffer = reinterpret_cast<const unsigned char *>(pt_buffer);

  crypto_aead_aes256gcm_encrypt(ct, &ct_len, 
                                casted_pt_buffer, pt.size(), 
                                nullptr, 0, NULL, nonce, key);
 
  std::string ct_str(reinterpret_cast<char*>(ct));
  return ct_str;
}

// decrypts ct (ciphertext) with key and returns pt (plaintext)
std::string Decrypt(unsigned char *key, std::string ct) {
  static unsigned char *pt = new unsigned char[MAX_LEN];
  long long unsigned pt_len = 0;
  const char *ct_buffer = ct.c_str();
  auto casted_ct_buffer = reinterpret_cast<const unsigned char *>(ct_buffer);
  
  crypto_aead_aes256gcm_decrypt(pt, &pt_len, NULL,
                                casted_ct_buffer, ct.size(), 
                                nullptr, 0, nonce, key);
  
  std::string pt_str(reinterpret_cast<char*>(pt));
  return pt_str;
}

int main(int argc, char **argv) {
  std::string message = "HELLO AES!";
  std::cout << "len: " << message.size() << std::endl;
  std::cout << "message: " << message << std::endl;
  std::cout << std::endl;

  // Initialize libsodium.
  if (sodium_init() == -1) {
    return 1;
  }

  // set nonce
  randombytes_buf(nonce, sizeof nonce);

  // Generate key.
  unsigned char *key = KeyGen();

  // encrypt message
  std::string ciphertext = Encrypt(key, message);

  // print ciphertext
  std::cout << "ciphertext: " << ciphertext << std::endl;
  std::cout << std::endl;

  // decrypt message
  std::string plaintext = Decrypt(key, ciphertext);

  // print plaintext
  std::cout << "plaintext: " << plaintext << std::endl;
  std::cout << std::endl;
}
