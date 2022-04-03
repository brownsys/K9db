#include <iostream>
#include <string>

// NOLINTNEXTLINE
#include "sodium.h"

int main(int argc, char **argv) {
  std::string message = "HELLO AES!";
  std::cout << "len: " << message.size() << std::endl;
  std::cout << "message: " << message << std::endl;
  std::cout << std::endl;

  // Initialize libsodium.
  sodium_init();

  // Nonce is a global random string that is the same for all keys and users.
  unsigned char nonce[crypto_aead_aes256gcm_NPUBBYTES];
  randombytes_buf(nonce, sizeof nonce);

  // Generate key.
  unsigned char key[crypto_aead_aes256gcm_KEYBYTES];
  crypto_aead_aes256gcm_keygen(key);

  // The encryption size is the same as the input plaintext size + a fixed
  // encryption overhead defined in crypto_aead_aes256gcm_ABYTES.
  size_t known_length = message.size() + crypto_aead_aes256gcm_ABYTES;
  unsigned char *ciphertext = new unsigned char[known_length];
  // The ciphertext buffer is sometimes fixed-sized ahead of knowing the size of
  // the plaintext message (so its size is an upperbound). For this reason,
  // Libsodium API can be given a pointer to a variable where it will write
  // the actual length of the ciphertext after encryption.
  // NOLINTNEXTLINE
  long long unsigned ciphertext_len = 0;

  // Encrypt.
  const char *message_buffer = message.c_str();
  auto casted_buffer = reinterpret_cast<const unsigned char *>(message_buffer);
  crypto_aead_aes256gcm_encrypt(ciphertext, &ciphertext_len, casted_buffer,
                                message.size(), nullptr, 0, NULL, nonce, key);

  // Print information.
  std::cout << "len: " << ciphertext_len << std::endl;
  std::cout << "len: " << known_length << std::endl;
  std::cout << "cipher: ";
  for (uint64_t i = 0; i < ciphertext_len; i++) {
    std::cout << ciphertext[i];
  }
  std::cout << std::endl;
  std::cout << std::endl;

  // Decrypt.
  // In the same way, the decryption buffer can be some fixed-size thing that is
  // defined ahead of time. The exact decrypted length will be written by
  // libsodium.
  char decrypted[1000];
  auto casted_decrypted = reinterpret_cast<unsigned char *>(decrypted);
  // NOLINTNEXTLINE
  long long unsigned decrypted_len = 0;
  crypto_aead_aes256gcm_decrypt(casted_decrypted, &decrypted_len, NULL,
                                ciphertext, ciphertext_len, nullptr, 0, nonce,
                                key);

  // Print information.
  std::cout << "len: " << decrypted_len << std::endl;
  std::cout << "message: " << std::string(decrypted, decrypted_len)
            << std::endl;
  std::cout << std::endl;
}
