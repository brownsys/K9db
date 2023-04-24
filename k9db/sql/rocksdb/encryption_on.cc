// clang-format off
// NOLINTNEXTLINE
#include "k9db/sql/rocksdb/encryption.h"
// clang-format on

#include <cassert>

#include "glog/logging.h"
// NOLINTNEXTLINE
#include "sodium.h"

namespace k9db {
namespace sql {
namespace rocks {

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

std::unique_ptr<unsigned char[]> DeserializeKey(const std::string &str,
                                                size_t size) {
  CHECK_EQ(str.size(), size);
  std::unique_ptr<char[]> ptr = std::make_unique<char[]>(size);
  std::memcpy(ptr.get(), str.data(), size);
  return std::unique_ptr<unsigned char[]>(
      reinterpret_cast<unsigned char *>(ptr.release()));
}

std::string SerializeKey(const unsigned char *key, size_t size) {
  return std::string(reinterpret_cast<const char *>(key), size);
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
  Offset size = EncryptedKey::ShardSize(this->data_);
  return rocksdb::Slice(this->data_.data(), size);
}

rocksdb::Slice EncryptedKey::GetPK() const {
  Offset offset = EncryptedKey::ShardSize(this->data_);
  size_t size = this->data_.size() - offset - sizeof(Offset);
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
      mtx_(),
      metadata_(nullptr) {
  // Initialize libsodium.
  assert(sodium_init() >= 0);

  // Generate random global key and nonce.
  crypto_aead_aes256gcm_keygen(this->global_key_.get());
  randombytes_buf(this->global_nonce_.get(), NONCE_SIZE);
}

// Initialization: load any previously created keys from persistent storage.
void EncryptionManager::Initialize(RocksdbMetadata *metadata) {
  this->metadata_ = metadata;

  // Load global key and nonce.
  std::optional<std::string> key = metadata->LoadGlobalKey();
  std::optional<std::string> nonce = metadata->LoadGlobalNonce();
  if (key.has_value()) {
    this->global_key_ = DeserializeKey(key.value(), KEY_SIZE);
    this->global_nonce_ = DeserializeKey(nonce.value(), NONCE_SIZE);
  } else {
    metadata->PersistGlobalKey(SerializeKey(this->global_key_.get(), KEY_SIZE));
    metadata->PersistGlobalNonce(
        SerializeKey(this->global_nonce_.get(), NONCE_SIZE));
  }

  // Load user keys.
  for (const auto &[user, key] : metadata->LoadUserKeys()) {
    this->keys_.emplace(user, DeserializeKey(key, KEY_SIZE));
  }
}

// Get the key of the given user.
const unsigned char *EncryptionManager::GetUserKey(
    const std::string &shard_name) const {
  util::SharedLock lock(&this->mtx_);
  return this->keys_.at(shard_name).get();
}

// Get the key of the given user.
// Create the key if that user does not yet have one.
const unsigned char *EncryptionManager::GetOrCreateUserKey(
    const std::string &shard_name) {
  util::SharedLock lock(&this->mtx_);
  auto &&[upgraded, condition] =
      lock.UpgradeIf([&]() { return this->keys_.count(shard_name) == 0; });
  if (condition) {
    std::unique_ptr<unsigned char[]> key =
        std::make_unique<unsigned char[]>(KEY_SIZE);
    crypto_aead_aes256gcm_keygen(key.get());
    auto [eit, _] = this->keys_.emplace(shard_name, std::move(key));
    const unsigned char *ptr = eit->second.get();
    // Unlock mutex before writing to persistent storage.
    upgraded->unlock();
    // Persist key.
    if (this->metadata_ != nullptr) {
      this->metadata_->PersistUserKey(shard_name, SerializeKey(ptr, KEY_SIZE));
    } else {
      LOG(WARNING) << "User keys are not persisted!";
    }
    return ptr;
  }
  return this->keys_.at(shard_name).get();
}

// Encryption of keys and values of records.
EncryptedKey EncryptionManager::EncryptKey(RocksdbSequence &&k) const {
  const unsigned char *nonce = this->global_nonce_.get();
  const unsigned char *key = this->global_key_.get();
  return EncryptedKey(Cipher(Encrypt(k.At(0), nonce, key)),
                      Cipher(Encrypt(k.At(1), nonce, key)));
}
EncryptedValue EncryptionManager::EncryptValue(const std::string &shard_name,
                                               RocksdbSequence &&v) {
  const unsigned char *nonce = this->global_nonce_.get();
  const unsigned char *key = this->GetOrCreateUserKey(shard_name);
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
RocksdbSequence EncryptionManager::DecryptValue(const std::string &shard_name,
                                                EncryptedValue &&v) const {
  const unsigned char *nonce = this->global_nonce_.get();
  const unsigned char *key = this->GetUserKey(shard_name);
  return RocksdbSequence(Decrypt(v.Data(), nonce, key));
}

// Encrypts a key for use with rocksdb Seek.
EncryptedPrefix EncryptionManager::EncryptSeek(util::ShardName &&seek) const {
  const unsigned char *nonce = this->global_nonce_.get();
  const unsigned char *key = this->global_key_.get();
  std::string cipher = Encrypt(seek.AsSlice(), nonce, key);
  EncryptedKey::Offset size = cipher.size();
  const char *ptr = reinterpret_cast<char *>(&size);
  cipher.push_back(ptr[0]);
  cipher.push_back(ptr[1]);
  return Cipher(std::move(cipher));
}

/*
 * K9dbPrefixTransform
 */
rocksdb::Slice K9dbPrefixTransform::Transform(const rocksdb::Slice &k) const {
  return rocksdb::Slice(k.data(), EncryptedKey::ShardSize(k));
}

/*
 * Comparator over ciphers
 */

namespace {

// Not exposed publicly. Must use K9dbComparator() to expose this.
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
const rocksdb::Comparator *K9dbComparator() {
  static EncryptedComparator comparator_instance;
  return &comparator_instance;
}

}  // namespace rocks
}  // namespace sql
}  // namespace k9db
