#include "pelton/sql/connections/encryption.h"

#include "glog/logging.h"

namespace pelton {
namespace sql {

#define __ROCKSSEP static_cast<char>(30)


#ifdef PELTON_ENCRYPTION
#define MAX_LEN 10000
// a global random string that is the same for all keys and users.
unsigned char nonce[crypto_aead_aes256gcm_NPUBBYTES];
#endif  // PELTON_ENCRYPTION

// initializes libsodium and sets nonce
void InitializeEncryption() {
#ifdef PELTON_ENCRYPTION
  // Initialize libsodium.
  sodium_init();

  // set nonce
  randombytes_buf(nonce, sizeof nonce);
#endif  // PELTON_ENCRYPTION
}

// generates random 256-bit key
unsigned char* KeyGen() {
#ifdef PELTON_ENCRYPTION
  unsigned char *key = new unsigned char[crypto_aead_aes256gcm_KEYBYTES];
  crypto_aead_aes256gcm_keygen(key);
  return key;
#else
  return nullptr;
#endif  // PELTON_ENCRYPTION
}

// encrypts pt (plaintext) with key and returns ct (ciphertext)
std::string Encrypt(unsigned char *key, std::string pt) {
#ifdef PELTON_ENCRYPTION
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
#else
  return pt;
#endif  // PELTON_ENCRYPTION
}

// decrypts ct (ciphertext) with key and returns pt (plaintext)
std::string Decrypt(unsigned char *key, std::string ct) {
#ifdef PELTON_ENCRYPTION
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

#else
  return ct;
#endif  // PELTON_ENCRYPTION
}

// encrypts user_id and id components of keys
std::string EncryptKey(unsigned char *key, std::string pt) {
#ifdef PELTON_ENCRYPTION
  rocksdb::Slice pt_slice = rocksdb::Slice(pt.data(), pt.size());
  std::string uid = Encrypt(key, ExtractColumn(pt_slice, 0).ToString());
  std::string id = Encrypt(key, ExtractColumn(pt_slice, 1).ToString());
  size_t uid_len = uid.length();
  return uid + id + std::string(reinterpret_cast<char *>(&uid_len), sizeof(size_t));
#else
  return pt;
#endif  // PELTON_ENCRYPTION
}

// decrypts user_id and id components of keys
std::string DecryptKey(unsigned char *key, std::string ct) {
#ifdef PELTON_ENCRYPTION
  const char *ct_str = ct.c_str();
  size_t uid_len_ptr = ct.size() - sizeof(size_t);
  size_t uid_len = *reinterpret_cast<const size_t *>(ct_str + uid_len_ptr);
  std::string uid = Decrypt(key, std::string(ct_str, uid_len));
  std::string id = Decrypt(key, std::string(ct_str + uid_len, uid_len_ptr - uid_len));
  return uid + __ROCKSSEP + id + __ROCKSSEP;
#else
  return ct;
#endif  // PELTON_ENCRYPTION
}

// encrypts user_id and id components of seek
std::string EncryptSeek(unsigned char *key, std::string pt) {
#ifdef PELTON_ENCRYPTION
  pt.pop_back();
  pt = Encrypt(key, pt);
  size_t uid_len = pt.size();
  pt += std::string(reinterpret_cast<char *>(&uid_len), sizeof(size_t));
  return pt;
#else
  return pt;
#endif  // PELTON_ENCRYPTION
}

// EncryptedPrefixTransform
rocksdb::Slice EncryptedPrefixTransform::Transform(
    const rocksdb::Slice &key) const {
#ifdef PELTON_ENCRYPTION
  const char *data = key.data();
  size_t offset = key.size() - sizeof(size_t);
  size_t prefix_len = *reinterpret_cast<const size_t *>(data + offset);
  return rocksdb::Slice(data, prefix_len);
#else
  return ExtractColumn(key, 0);
#endif  // PELTON_ENCRYPTION
}

int EncryptedComparator::Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const {
#ifdef PELTON_ENCRYPTION
  const char *a_data = a.data();
  size_t a_offset = a.size() - sizeof(size_t);
  size_t a_len = *reinterpret_cast<const size_t *>(a_data + a_offset);
  rocksdb::Slice a_user(a_data, a_len);
  rocksdb::Slice a_pk(a_data + a_len, a_offset - a_len);

  const char *b_data = b.data();
  size_t b_offset = b.size() - sizeof(size_t);
  size_t b_len = *reinterpret_cast<const size_t *>(b_data + b_offset);
  rocksdb::Slice b_user(b_data, b_len);
  rocksdb::Slice b_pk(b_data + b_len, b_offset - b_len);

  int r = this->bytewise_->Compare(a_user, b_user);
  if (r == 0) {
    return this->bytewise_->Compare(a_pk, b_pk);
  }
  return r;
#else
  return this->bytewise_->Compare(a, b);
#endif  // PELTON_ENCRYPTION
}

const rocksdb::Comparator *EncryptedComparator::Instance() {
#ifdef PELTON_ENCRYPTION
  return new EncryptedComparator();
#else
  return rocksdb::BytewiseComparator();
#endif  // PELTON_ENCRYPTION
}

}  // namespace sql
}  // namespace pelton
