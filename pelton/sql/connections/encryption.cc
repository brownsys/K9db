#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>
#include <chrono>
#include <assert.h>
#include <ctime>
#include <unistd.h>

// NOLINTNEXTLINE
#include "sodium.h"

#define MAX_LEN 10000

#define NUM_TESTS 10000
#define INPUT_SIZE 100

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
 
  std::string ct_str(reinterpret_cast<char const*>(ct), ct_len);
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
  
  std::string pt_str(reinterpret_cast<char const*>(pt), pt_len);
  return pt_str;
}

std::string gen_random_str(const int len) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    std::string tmp_s;
    tmp_s.reserve(len);

    for (int i = 0; i < len; ++i) {
        tmp_s += alphanum[rand() % (sizeof(alphanum) - 1)];
    }
    
    return tmp_s;
}

int main(int argc, char **argv) {

  // Initialize libsodium.
  if (sodium_init() == -1) {
    return 1;
  }

  // set nonce
  randombytes_buf(nonce, sizeof nonce);

  // Generate key.
  unsigned char *key = KeyGen();

  // Timers for recording encryption and decryption times
  uint64_t encryption_time = 0;
  uint64_t decryption_time = 0;

  // Encrypt and Decrypt NUM_TESTS random strings
  std::clog << "Performing encryptions and decryptions...\n";

  for (int i = 0; i < NUM_TESTS; i++) {
    std::string line = gen_random_str(INPUT_SIZE);

    // Encrypt line, record time
    auto encr_start = std::chrono::steady_clock::now();
    std::string ciphertext = Encrypt(key, line);
    auto encr_stop = std::chrono::steady_clock::now();
    encryption_time += std::chrono::duration_cast<std::chrono::nanoseconds>(
      encr_stop - encr_start).count();

    // Decrypt line, record time
    auto decr_start = std::chrono::steady_clock::now();
    std::string plaintext = Decrypt(key, ciphertext);
    auto decr_stop = std::chrono::steady_clock::now();
    decryption_time += std::chrono::duration_cast<std::chrono::nanoseconds>(
      decr_stop - decr_start).count();

    // assert decryption correctness
    assert(line == plaintext);
  }

  // Print times for encryptions and decryptions
  std::cout << 
  "Encryptions time: " << 
    static_cast<double>(encryption_time) / 1000000000 << " s\n" 
  "Decryptions time: " <<
    static_cast<double>(decryption_time) / 1000000000 << " s\n";

  return 0;






  // std::string message = "HELLO AES!";
  // std::cout << "len: " << message.size() << std::endl;
  // std::cout << "message: " << message << std::endl;
  // std::cout << std::endl;
  // // Initialize libsodium.
  // sodium_init();
  // // set nonce
  // randombytes_buf(nonce, sizeof nonce);
  // // Generate key.
  // unsigned char *key = KeyGen();
  // // encrypt message
  // std::string ciphertext = Encrypt(key, message);
  // // print ciphertext
  // std::cout << "ciphertext: " << ciphertext << std::endl;
  // std::cout << std::endl;
  // // decrypt message
  // std::string plaintext = Decrypt(key, ciphertext);
  // // print plaintext
  // std::cout << "plaintext: " << plaintext << std::endl;
  // std::cout << std::endl;
}
