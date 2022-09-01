#include "pelton/sql/rocksdb/encryption.h"

#ifdef PELTON_ENCRYPTION

#include <cassert>

#include "glog/logging.h"
// NOLINTNEXTLINE
#include "sodium.h"

namespace pelton {
namespace sql {

#define KEY_SIZE crypto_aead_aes256gcm_KEYBYTES
#define NONCE_SIZE crypto_aead_aes256gcm_NPUBBYTES
#define CIPHER_OVERHEAD crypto_aead_aes256gcm_ABYTES
#define PLAIN_MAX_LEN 10000

#define ENCRYPT(dst, dsz, src, sz, nonce, key) \
  crypto_aead_aes256gcm_encrypt(dst, dsz, src, sz, nullptr, 0, NULL, nonce, key)
#define DECRYPT(dst, dsz, src, sz, nonce, key) \
  crypto_aead_aes256gcm_decrypt(dst, dsz, NULL, src, sz, nullptr, 0, nonce, key)

namespace {

std::string Encrypt(rocksdb::Slice input, const unsigned char *nonce,
                    const unsigned char *key) {
  const unsigned char *input_buf =
      reinterpret_cast<const unsigned char *>(input.data());

  // Allocate memory.
  size_t capacity = input.size() + CIPHER_OVERHEAD;
  std::unique_ptr<unsigned char[]> dst =
      std::make_unique<unsigned char[]>(capacity);

  // NOLINTNEXTLINE
  unsigned long long size;
  if (ENCRYPT(dst.get(), &size, input_buf, input.size(), nonce, key) != 0) {
    LOG(FATAL) << "Cannot encrypt!";
  }

  return std::string(reinterpret_cast<const char *>(dst.get()), size);
}

std::string Decrypt(rocksdb::Slice input, const unsigned char *nonce,
                    const unsigned char *key) {
  if (input.size() < CIPHER_OVERHEAD) {
    LOG(FATAL) << "Cipher too short to decrypt!";
  }

  const unsigned char *input_buf =
      reinterpret_cast<const unsigned char *>(input.data());

  // Allocate memory for plaintext.
  std::unique_ptr<unsigned char[]> dst =
      std::make_unique<unsigned char[]>(PLAIN_MAX_LEN);

  // NOLINTNEXTLINE
  unsigned long long size;
  if (DECRYPT(dst.get(), &size, input_buf, input.size(), nonce, key) != 0) {
    LOG(FATAL) << "Cannot decrypt!";
  }

  return std::string(reinterpret_cast<const char *>(dst.get()), size);
}

}  // namespace

/*
 * EncryptedKey
 */

// Constructed after encrypting.
EncryptedKey::EncryptedKey(Cipher &&shard_cipher, const Cipher &pk_cipher)
    : data_(shard_cipher.Release()) {
  Offset sz = this->data_.size();
  rocksdb::Slice pk = pk_cipher.Data();
  this->data_.append(pk.data(), pk.size());
  char *ptr = reinterpret_cast<char *>(&sz);
  this->data_.push_back(ptr[0]);
  this->data_.push_back(ptr[1]);
}

rocksdb::Slice EncryptedKey::GetShard() const {
  uint16_t size = EncryptedKey::ShardSize(this->data_);
  return rocksdb::Slice(this->data_.data(), size);
}

rocksdb::Slice EncryptedKey::GetPK() const {
  uint16_t offset = EncryptedKey::ShardSize(this->data_);
  size_t size = this->data_.size() - offset;
  return rocksdb::Slice(this->data_.data() + offset, size);
}

EncryptedKey::Offset EncryptedKey::ShardSize(const rocksdb::Slice &slice) {
  const char *ptr = slice.data() + (slice.size() - sizeof(Offset));
  return *reinterpret_cast<const Offset *>(ptr);
}

/*
 * EncryptionManager
 */

// Construct encryption manager.
EncryptionManager::EncryptionManager()
    : global_key_(std::make_unique<unsigned char[]>(KEY_SIZE)),
      global_nonce_(std::make_unique<unsigned char[]>(NONCE_SIZE)),
      keys_(),
      mtx_() {
  // Initialize libsodium.
  assert(sodium_init() >= 0);

  // Generate random global key and nonce.
  crypto_aead_aes256gcm_keygen(this->global_key_.get());
  randombytes_buf(this->global_nonce_.get(), NONCE_SIZE);
}

// Get the key of the given user.
const unsigned char *EncryptionManager::GetUserKey(
    const std::string &user_id) const {
  shards::SharedLock lock(&this->mtx_);
  return this->keys_.at(user_id).get();
}

// Get the key of the given user.
// Create the key if that user does not yet have one.
const unsigned char *EncryptionManager::GetOrCreateUserKey(
    const std::string &user_id) {
  shards::SharedLock lock(&this->mtx_);
  auto it = this->keys_.find(user_id);
  if (it == this->keys_.end()) {
    shards::UniqueLock upgraded(std::move(lock));
    it = this->keys_.find(user_id);
    if (it == this->keys_.end()) {
      std::unique_ptr<unsigned char[]> key =
          std::make_unique<unsigned char[]>(KEY_SIZE);
      crypto_aead_aes256gcm_keygen(key.get());
      auto [eit, _] = this->keys_.emplace(user_id, std::move(key));
      return eit->second.get();
    }
    return it->second.get();
  }
  return it->second.get();
}

// Encryption of keys and values of records.
EncryptedKey EncryptionManager::EncryptKey(RocksdbSequence &&k) const {
  const unsigned char *nonce = this->global_nonce_.get();
  const unsigned char *key = this->global_key_.get();
  return EncryptedKey(Cipher(Encrypt(k.At(0), nonce, key)),
                      Cipher(Encrypt(k.At(1), nonce, key)));
}
EncryptedValue EncryptionManager::EncryptValue(const std::string &user_id,
                                               RocksdbSequence &&v) {
  const unsigned char *nonce = this->global_nonce_.get();
  const unsigned char *key = this->GetOrCreateUserKey(user_id);
  return Cipher(Encrypt(v.Data(), nonce, key));
}

// Decryption of records.
RocksdbSequence EncryptionManager::DecryptKey(EncryptedKey &&k) const {
  const unsigned char *nonce = this->global_nonce_.get();
  const unsigned char *key = this->global_key_.get();

  RocksdbSequence result;
  result.AppendEncoded(Decrypt(k.GetShard(), nonce, key));
  result.AppendEncoded(Decrypt(k.GetPK(), nonce, key));
  return result;
}
RocksdbSequence EncryptionManager::DecryptValue(const std::string &user_id,
                                                EncryptedValue &&v) const {
  const unsigned char *nonce = this->global_nonce_.get();
  const unsigned char *key = this->GetUserKey(user_id);
  return RocksdbSequence(Decrypt(v.Data(), nonce, key));
}

// Encrypts a key for use with rocksdb Seek.
Cipher EncryptionManager::EncryptSeek(std::string &&seek_key) const {
  const unsigned char *nonce = this->global_nonce_.get();
  const unsigned char *key = this->global_key_.get();
  return Cipher(Encrypt(seek_key, nonce, key));
}

/*
 * PeltonPrefixTransform
 */
rocksdb::Slice PeltonPrefixTransform::Transform(const rocksdb::Slice &k) const {
  return rocksdb::Slice(k.data(), EncryptedKey::ShardSize(k));
}

/*
 * Comparator over ciphers
 */

namespace {

// Not exposed publicly. Must use PeltonComparator() to expose this.
class EncryptedComparator : public rocksdb::Comparator {
 public:
  EncryptedComparator()
      : rocksdb::Comparator(), bytewise_(rocksdb::BytewiseComparator()) {}

  const char *Name() const override { return "EncryptedComparator"; }

  // Advanced functions: these are used to reduce the space requirements
  // for internal data structures like index blocks.
  void FindShortestSeparator(std::string *start,
                             const rocksdb::Slice &limit) const override {}
  void FindShortSuccessor(std::string *key) const override {}

  // Three-way comparison.  Returns value:
  //   < 0 iff "a" < "b",
  //   == 0 iff "a" == "b",
  //   > 0 iff "a" > "b"
  int Compare(const rocksdb::Slice &a, const rocksdb::Slice &b) const override {
    EncryptedKey::Offset a_offset = EncryptedKey::ShardSize(a);
    rocksdb::Slice a_shard(a.data(), a_offset);

    EncryptedKey::Offset b_offset = EncryptedKey::ShardSize(b);
    rocksdb::Slice b_shard(b.data(), b_offset);
    int result = this->bytewise_->Compare(a_shard, b_shard);
    if (result != 0) {
      return result;
    }

    rocksdb::Slice a_pk(a.data() + a_offset,
                        a.size() - a_offset - sizeof(EncryptedKey::Offset));
    rocksdb::Slice b_pk(b.data() + b_offset,
                        b.size() - b_offset - sizeof(EncryptedKey::Offset));
    return this->bytewise_->Compare(a_pk, b_pk);
  }

 private:
  const rocksdb::Comparator *bytewise_;
};

}  // namespace

// static instance.
const rocksdb::Comparator *PeltonComparator() {
  static EncryptedComparator comparator_instance;
  return &comparator_instance;
}

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_ENCRYPTION
