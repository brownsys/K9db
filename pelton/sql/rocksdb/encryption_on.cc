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

// EncryptedRecord.
rocksdb::Slice EncryptedRecord::GetShard() const {
  size_t shard_offset = this->key_.size() - sizeof(size_t);
  const char *offset_ptr = this->key_.data() + shard_offset;
  size_t shard_size = *reinterpret_cast<const size_t *>(offset_ptr);
  return rocksdb::Slice(this->key_.data(), shard_size);
}

rocksdb::Slice EncryptedRecord::GetPK() const {
  size_t shard_offset = this->key_.size() - sizeof(size_t);
  const char *offset_ptr = this->key_.data() + shard_offset;
  size_t shard_size = *reinterpret_cast<const size_t *>(offset_ptr);
  return rocksdb::Slice(this->key_.data() + shard_size,
                        this->key_.size() - sizeof(size_t) - shard_size);
}

// EncryptionManager.
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

// Get the key of the given user, create the key for that user if it does not
// exist.
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

const unsigned char *EncryptionManager::GetUserKey(
    const std::string &user_id) const {
  shards::SharedLock lock(&this->mtx_);
  return this->keys_.at(user_id).get();
}

// Encrypts a record and returns the encrypt key and value.
EncryptedRecord EncryptionManager::EncryptRecord(const std::string &user_id,
                                                 RocksdbRecord &&r) {
  // Get user key.
  const unsigned char *key = this->GetOrCreateUserKey(user_id);
  const unsigned char *nonce = this->global_nonce_.get();

  // Allocate memory for value cipher.
  size_t val_size = r.Value().size();
  std::unique_ptr<unsigned char[]> ct =
      std::make_unique<unsigned char[]>(val_size + CIPHER_OVERHEAD);

  // Encrypt value.
  // NOLINTNEXTLINE
  unsigned long long ct_len = 0;
  auto val_buf = reinterpret_cast<const unsigned char *>(r.Value().data());
  if (crypto_aead_aes256gcm_encrypt(ct.get(), &ct_len, val_buf, val_size,
                                    nullptr, 0, NULL, nonce, key) != 0) {
    LOG(FATAL) << "Cannot encrypt value!";
  }

  char *encrypted_value = reinterpret_cast<char *>(ct.get());
  return EncryptedRecord(this->EncryptKey(r.Key()),
                         std::string(encrypted_value, ct_len));
}

// Encrypts a fully encoded key with a shardname and a pk.
std::string EncryptionManager::EncryptKey(rocksdb::Slice key) {
  const unsigned char *nonce = this->global_nonce_.get();

  // Allocate memory for encrypted key.
  rocksdb::Slice shard = ExtractColumn(key, 0);
  rocksdb::Slice pk = ExtractColumn(key, 1);
  size_t max_key_ct_size =
      shard.size() + pk.size() + 2 * CIPHER_OVERHEAD + sizeof(size_t);
  std::unique_ptr<unsigned char[]> key_ct =
      std::make_unique<unsigned char[]>(max_key_ct_size);

  // Encrypt Shard.
  // NOLINTNEXTLINE
  unsigned long long key_ct_size = 0;
  auto shard_buf = reinterpret_cast<const unsigned char *>(shard.data());
  if (crypto_aead_aes256gcm_encrypt(key_ct.get(), &key_ct_size, shard_buf,
                                    shard.size(), nullptr, 0, NULL, nonce,
                                    this->global_key_.get()) != 0) {
    LOG(FATAL) << "Cannot encrypt shard!";
  }

  // Encrypt PK.
  size_t shard_size = key_ct_size;
  auto pk_buf = reinterpret_cast<const unsigned char *>(pk.data());
  if (crypto_aead_aes256gcm_encrypt(key_ct.get() + shard_size, &key_ct_size,
                                    pk_buf, pk.size(), nullptr, 0, NULL, nonce,
                                    this->global_key_.get()) != 0) {
    LOG(FATAL) << "Cannot encrypt pk!";
  }

  // Add length of cipher shard.
  memcpy(key_ct.get() + shard_size + key_ct_size, &shard_size, sizeof(size_t));

  char *encrypted_key = reinterpret_cast<char *>(key_ct.get());
  return std::string(encrypted_key, shard_size + key_ct_size + sizeof(size_t));
}

// Decrypts an encrypted record value.
RocksdbRecord EncryptionManager::DecryptRecord(const std::string &user_id,
                                               EncryptedRecord &&r) const {
  // Decrypt key!
  std::string decrypted_key = "";
  if (r.Key().size() > 0) {
    decrypted_key = this->DecryptKey(r.ReleaseKey());
  }

  // Get encryption key.
  const unsigned char *key = this->GetUserKey(user_id);
  const unsigned char *nonce = this->global_nonce_.get();

  // Allocate memory for plaintext.
  std::unique_ptr<unsigned char[]> pt =
      std::make_unique<unsigned char[]>(PLAIN_MAX_LEN);

  // Decrypt value.
  rocksdb::Slice val = r.Value();
  // NOLINTNEXTLINE
  unsigned long long pt_len = 0;
  auto val_buf = reinterpret_cast<const unsigned char *>(val.data());
  if (val.size() < CIPHER_OVERHEAD ||
      crypto_aead_aes256gcm_decrypt(pt.get(), &pt_len, NULL, val_buf,
                                    val.size(), nullptr, 0, nonce, key) != 0) {
    LOG(FATAL) << "Cannot decrypt value!";
  }

  std::string decrypted_value(reinterpret_cast<char const *>(pt.get()), pt_len);

  // Return as a (decrypted) record.
  return RocksdbRecord(std::move(decrypted_key), std::move(decrypted_value));
}

// Decrypts an encrypted record key and returns its components.
std::string EncryptionManager::DecryptKey(std::string &&cipher) const {
  // Get key.
  const unsigned char *key = this->global_key_.get();
  const unsigned char *nonce = this->global_nonce_.get();

  // Allocate memory for plaintext.
  std::unique_ptr<unsigned char[]> pt =
      std::make_unique<unsigned char[]>(PLAIN_MAX_LEN);

  // Figure out component offsets.
  EncryptedRecord record(std::move(cipher));
  rocksdb::Slice shard = record.GetShard();
  rocksdb::Slice pk = record.GetPK();

  // Decrypt shard.
  const unsigned char *shard_buf =
      reinterpret_cast<const unsigned char *>(shard.data());
  // NOLINTNEXTLINE
  unsigned long long shardlen = 0;
  if (shard.size() < CIPHER_OVERHEAD ||
      crypto_aead_aes256gcm_decrypt(pt.get(), &shardlen, NULL, shard_buf,
                                    shard.size(), nullptr, 0, nonce,
                                    key) != 0) {
    LOG(FATAL) << "Cannot decrypt shard!";
  }
  pt[shardlen] = __ROCKSSEP;

  // Decrypt PK.
  const unsigned char *pk_buf =
      reinterpret_cast<const unsigned char *>(pk.data());
  // NOLINTNEXTLINE
  unsigned long long pklen = 0;
  if (pk.size() < CIPHER_OVERHEAD ||
      crypto_aead_aes256gcm_decrypt(pt.get() + shardlen + 1, &pklen, NULL,
                                    pk_buf, pk.size(), nullptr, 0, nonce,
                                    key) != 0) {
    LOG(FATAL) << "Cannot decrypt PK!";
  }
  pt[shardlen + 1 + pklen] = __ROCKSSEP;

  return std::string(reinterpret_cast<char *>(pt.get()), shardlen + pklen + 2);
}

// encrypts user_id and id components of seek.
std::string EncryptionManager::EncryptSeek(std::string &&seek_key) const {
  // Get user key.
  const unsigned char *key = this->global_key_.get();
  const unsigned char *nonce = this->global_nonce_.get();

  // Allocate memory for value cipher.
  std::unique_ptr<unsigned char[]> ct =
      std::make_unique<unsigned char[]>(seek_key.size() + CIPHER_OVERHEAD);

  // Encrypt value.
  // NOLINTNEXTLINE
  unsigned long long ct_len = 0;
  auto pt_buffer = reinterpret_cast<const unsigned char *>(seek_key.data());
  if (crypto_aead_aes256gcm_encrypt(ct.get(), &ct_len, pt_buffer,
                                    seek_key.size(), nullptr, 0, NULL, nonce,
                                    key) != 0) {
    LOG(FATAL) << "Cannot encrypt!";
  }

  return std::string(reinterpret_cast<char *>(ct.get()), ct_len);
}

// PeltonPrefixTransform.
rocksdb::Slice PeltonPrefixTransform::Transform(const rocksdb::Slice &k) const {
  const char *data = k.data();
  size_t offset = k.size() - sizeof(size_t);
  size_t prefix_len = *reinterpret_cast<const size_t *>(data + offset);
  return rocksdb::Slice(data, prefix_len);
}

// Pelton's Comparator.
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
