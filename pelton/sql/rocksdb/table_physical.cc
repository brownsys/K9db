#include "pelton/sql/rocksdb/table_physical.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "glog/logging.h"
#include "pelton/sql/rocksdb/encode.h"
#include "pelton/util/status.h"
#include "rocksdb/options.h"

namespace pelton {
namespace sql {
namespace rocks {

/*
 * RocksdbStream
 */

using RocksdbStreamIterator = RocksdbStream::Iterator;

// Construct end iterator.
RocksdbStreamIterator::Iterator() : its_(), loc_(0) {}

// Construct iterator given a ready rocksdb iterator (with some seek performed).
RocksdbStreamIterator::Iterator(std::vector<rocksdb::Iterator *> its)
    : its_(std::move(its)), loc_(0) {
  this->EnsureValid();
}

// Next key/value pair.
RocksdbStreamIterator &RocksdbStreamIterator::operator++() {
  if (this->loc_ < this->its_.size()) {
    this->its_.at(this->loc_)->Next();
    this->EnsureValid();
  }
  return *this;
}

// Equality of underlying iterator.
bool RocksdbStreamIterator::operator==(const RocksdbStreamIterator &o) const {
  CHECK(this->loc_ >= this->its_.size() || o.loc_ >= o.its_.size());
  return (this->loc_ >= this->its_.size()) == (o.loc_ >= o.its_.size());
}
bool RocksdbStreamIterator::operator!=(const RocksdbStreamIterator &o) const {
  CHECK(this->loc_ >= this->its_.size() || o.loc_ >= o.its_.size());
  return (this->loc_ >= this->its_.size()) != (o.loc_ >= o.its_.size());
}

// Get current key/value pair.
RocksdbStreamIterator::value_type RocksdbStreamIterator::operator*() const {
  return RocksdbStreamIterator::value_type(
      this->its_.at(this->loc_)->key(),
      Cipher::FromDB(this->its_.at(this->loc_)->value().ToString()));
}

// When the iterator is consumed, set it to nullptr.
void RocksdbStreamIterator::EnsureValid() {
  for (; this->loc_ < this->its_.size(); this->loc_++) {
    if (this->its_.at(this->loc_)->Valid()) {
      return;
    }
  }
}

// To an actual vector. Consume this stream.
std::vector<std::pair<EncryptedKey, EncryptedValue>> RocksdbStream::ToVector() {
  std::vector<std::pair<EncryptedKey, EncryptedValue>> result;
  auto end = this->end();
  for (auto it = this->begin(); it != end; ++it) {
    result.emplace_back(*it);
  }
  return result;
}

/*
 * RocksdbPhysicalSeparator.
 */

RocksdbPhysicalSeparator::RocksdbPhysicalSeparator(
    rocksdb::DB *db, const std::string &table_name)
    : db_(db), table_name_(table_name) {
#ifndef PELTON_PHYSICAL_SEPARATION
  // Create rocksdb table.
  rocksdb::ColumnFamilyHandle *handle;
  rocksdb::ColumnFamilyOptions options;
  options.OptimizeLevelStyleCompaction();
  options.prefix_extractor.reset(new PeltonPrefixTransform());
  options.comparator = PeltonComparator();
  PANIC(this->db_->CreateColumnFamily(options, table_name, &handle));
  this->handle_ = std::unique_ptr<rocksdb::ColumnFamilyHandle>(handle);
#endif  // PELTON_PHYSICAL_SEPARATION
}

// Put/Delete API.
void RocksdbPhysicalSeparator::Put(const EncryptedKey &key,
                                   const EncryptedValue &value,
                                   RocksdbTransaction *txn) {
  rocksdb::ColumnFamilyHandle *cf = this->GetOrCreateCf(key);
  txn->Put(cf, key.Data(), value.Data());
}

void RocksdbPhysicalSeparator::Delete(const EncryptedKey &key,
                                      RocksdbTransaction *txn) {
  std::optional<rocksdb::ColumnFamilyHandle *> cf = this->GetCf(key);
  if (cf.has_value()) {
    txn->Delete(cf.value(), key.Data());
  }
}

// Get and MultiGet.
std::optional<EncryptedValue> RocksdbPhysicalSeparator::Get(
    const EncryptedKey &key, bool read, const RocksdbTransaction *txn) const {
  std::optional<rocksdb::ColumnFamilyHandle *> cf = this->GetCf(key);
  if (!cf.has_value()) {
    return {};
  }
  std::optional<std::string> data;
  if (read) {
    data = txn->Get(cf.value(), key.Data());
  } else {
    data = txn->GetForUpdate(cf.value(), key.Data());
  }
  if (data.has_value()) {
    return Cipher::FromDB(std::move(data.value()));
  }
  return {};
}

std::vector<std::optional<EncryptedValue>> RocksdbPhysicalSeparator::MultiGet(
    const std::vector<EncryptedKey> &keys, bool read,
    const RocksdbTransaction *txn) const {
  if (keys.size() == 0) {
    return {};
  }

  // Find the CF of every key.
  std::unordered_map<std::string, std::vector<size_t>> map;
  for (size_t i = 0; i < keys.size(); i++) {
    std::string shard;
#ifdef PELTON_PHYSICAL_SEPARATION
    shard = this->transform_.Transform(keys.at(i).Data()).ToString();
#endif  // PELTON_PHYSICAL_SEPARATION
    std::optional<rocksdb::ColumnFamilyHandle *> cf = this->GetCf(keys.at(i));
    if (cf.has_value()) {
      map[shard].push_back(i);
    }
  }

  // MultiGet within every family.
  std::vector<std::optional<EncryptedValue>> result(keys.size());
  for (const auto &[_, vec] : map) {
    rocksdb::ColumnFamilyHandle *cf = this->GetOrErrorCf(keys.at(vec.front()));

    std::vector<rocksdb::Slice> slices;
    slices.reserve(vec.size());
    for (size_t i : vec) {
      slices.push_back(keys.at(i).Data());
    }

    // Use MultiGet.
    std::vector<std::optional<std::string>> data;
    if (read) {
      data = txn->MultiGet(cf, slices);
    } else {
      data = txn->MultiGetForUpdate(cf, slices);
    }

    // Transform to Ciphers.
    for (size_t i = 0; i < vec.size(); i++) {
      std::optional<std::string> opt = data.at(i);
      if (opt.has_value()) {
        result[vec.at(i)] = Cipher::FromDB(std::move(opt.value()));
      }
    }
  }

  return result;
}

// Read all the data.
RocksdbStream RocksdbPhysicalSeparator::GetAll(
    const RocksdbTransaction *txn) const {
#ifdef PELTON_PHYSICAL_SEPARATION
  util::SharedLock lock(&this->mtx_);
  std::vector<std::unique_ptr<rocksdb::Iterator>> its;
  its.reserve(this->handles_.size());
  for (const auto &[_, cf] : this->handles_) {
    std::unique_ptr<rocksdb::Iterator> it = txn->Iterate(cf.get(), false);
    it->SeekToFirst();
    its.push_back(std::move(it));
  }
  return RocksdbStream(std::move(its));
#else
  std::unique_ptr<rocksdb::Iterator> it =
      txn->Iterate(this->handle_.get(), false);
  it->SeekToFirst();
  return RocksdbStream(std::move(it));
#endif
}

// Read all data from shard.
RocksdbStream RocksdbPhysicalSeparator::GetShard(
    const EncryptedPrefix &shard_name, const RocksdbTransaction *txn) const {
  std::optional<rocksdb::ColumnFamilyHandle *> cf = this->GetCf(shard_name);
  if (cf.has_value()) {
    // Iterator that spans the whole shard prefix.
    std::unique_ptr<rocksdb::Iterator> it = txn->Iterate(cf.value(), true);
    it->Seek(shard_name.Data());
    return RocksdbStream(std::move(it));
  }
  return RocksdbStream(std::vector<std::unique_ptr<rocksdb::Iterator>>());
}

#ifdef PELTON_PHYSICAL_SEPARATION
// Get or create a new column family by shard.
template <typename T>
rocksdb::ColumnFamilyHandle *RocksdbPhysicalSeparator::GetOrCreateCf(
    const T &cipher) {
  std::string shard = this->transform_.Transform(cipher.Data()).ToString();
  util::SharedLock lock(&this->mtx_);
  auto &&[upgraded, condition] =
      lock.UpgradeIf([&]() { return this->handles_.count(shard) == 0; });
  if (condition) {
    std::string name = this->table_name_ + "_" + shard;
    // Create rocksdb table.
    rocksdb::ColumnFamilyHandle *handle;
    rocksdb::ColumnFamilyOptions options;
    options.OptimizeLevelStyleCompaction();
    options.prefix_extractor.reset(new PeltonPrefixTransform());
    options.comparator = PeltonComparator();
    PANIC(this->db_->CreateColumnFamily(options, name, &handle));
    this->handles_.emplace(shard, handle);
    return handle;
  }
  return this->handles_.at(shard).get();
}
// Get or error out.
template <typename T>
rocksdb::ColumnFamilyHandle *RocksdbPhysicalSeparator::GetOrErrorCf(
    const T &cipher) const {
  std::string shard = this->transform_.Transform(cipher.Data()).ToString();
  util::SharedLock lock(&this->mtx_);
  auto it = this->handles_.find(shard);
  CHECK(it != this->handles_.end());
  return it->second.get();
}
// Get or optional.
template <typename T>
std::optional<rocksdb::ColumnFamilyHandle *> RocksdbPhysicalSeparator::GetCf(
    const T &cipher) const {
  std::string shard = this->transform_.Transform(cipher.Data()).ToString();
  util::SharedLock lock(&this->mtx_);
  auto it = this->handles_.find(shard);
  if (it != this->handles_.end()) {
    return it->second.get();
  }
  return {};
}
#else
template <typename T>
rocksdb::ColumnFamilyHandle *RocksdbPhysicalSeparator::GetOrCreateCf(
    const T &cipher) {
  return this->handle_.get();
}
template <typename T>
rocksdb::ColumnFamilyHandle *RocksdbPhysicalSeparator::GetOrErrorCf(
    const T &cipher) const {
  return this->handle_.get();
}
template <typename T>
std::optional<rocksdb::ColumnFamilyHandle *> RocksdbPhysicalSeparator::GetCf(
    const T &cipher) const {
  return this->handle_.get();
}
#endif  // PELTON_PHYSICAL_SEPARATION

}  // namespace rocks
}  // namespace sql
}  // namespace pelton
