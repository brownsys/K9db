#ifndef PELTON_SQL_ROCKSDB_TABLE_PHYSICAL_H__
#define PELTON_SQL_ROCKSDB_TABLE_PHYSICAL_H__

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "pelton/sql/rocksdb/encryption.h"
#include "pelton/sql/rocksdb/transaction.h"
#include "pelton/util/upgradable_lock.h"
#include "rocksdb/db.h"
#include "rocksdb/table.h"

namespace pelton {
namespace sql {
namespace rocks {

class RocksdbStream {
 public:
  // Iterator class.
  class Iterator {
   public:
    // Iterator traits
    using difference_type = int64_t;
    using value_type = std::pair<EncryptedKey, EncryptedValue>;
    using reference = std::pair<EncryptedKey, EncryptedValue> &;
    using pointer = std::pair<EncryptedKey, EncryptedValue> *;
    using iterator_category = std::input_iterator_tag;

    Iterator &operator++();

    bool operator==(const Iterator &o) const;
    bool operator!=(const Iterator &o) const;

    // Access current element.
    value_type operator*() const;

   private:
    std::vector<rocksdb::Iterator *> its_;
    size_t loc_;

    Iterator();
    explicit Iterator(std::vector<rocksdb::Iterator *> its);

    void EnsureValid();

    friend RocksdbStream;
  };

  // Construct from one unique ptr or a vector.
  explicit RocksdbStream(std::unique_ptr<rocksdb::Iterator> &&it) : its_() {
    this->its_.push_back(std::move(it));
  }
  explicit RocksdbStream(std::vector<std::unique_ptr<rocksdb::Iterator>> &&its)
      : its_(std::move(its)) {}

  RocksdbStream::Iterator begin() const {
    std::vector<rocksdb::Iterator *> ptrs;
    for (const auto &it : its_) {
      ptrs.push_back(it.get());
    }
    return Iterator(std::move(ptrs));
  }
  RocksdbStream::Iterator end() const { return Iterator(); }

  // To an actual vector. Consumes this vector.
  std::vector<std::pair<EncryptedKey, EncryptedValue>> ToVector();

 private:
  std::vector<std::unique_ptr<rocksdb::Iterator>> its_;
};

class RocksdbPhysicalSeparator {
 public:
  RocksdbPhysicalSeparator(rocksdb::DB *db, const std::string &table_name);

  // Put/Delete API.
  void Put(const EncryptedKey &key, const EncryptedValue &value,
           RocksdbTransaction *txn);

  void Delete(const EncryptedKey &key, RocksdbTransaction *txn);

  // Get and MultiGet.
  std::optional<EncryptedValue> Get(const EncryptedKey &key, bool read,
                                    const RocksdbTransaction *txn) const;
  std::vector<std::optional<EncryptedValue>> MultiGet(
      const std::vector<EncryptedKey> &keys, bool read,
      const RocksdbTransaction *txn) const;

  // Read all the data.
  RocksdbStream GetAll(const RocksdbTransaction *txn) const;

  // Read all data from shard.
  RocksdbStream GetShard(const EncryptedPrefix &shard_name,
                         const RocksdbTransaction *txn) const;

 private:
  using Handle = std::unique_ptr<rocksdb::ColumnFamilyHandle>;

  rocksdb::DB *db_;
  std::string table_name_;

  // If physically separated, we create a different Column Family per shard,
  // otherwise they are all in the same shard.
#ifdef PELTON_PHYSICAL_SEPARATION
  mutable util::UpgradableMutex mtx_;
  std::unordered_map<std::string, Handle> handles_;
  // Extracts shard from keys.
  PeltonPrefixTransform transform_;
#else
  Handle handle_;
#endif  // PELTON_PHYSICAL_SEPARATION

  // Get or create a new column family by shard.
  template <typename T>
  rocksdb::ColumnFamilyHandle *GetOrCreateCf(const T &shard);

  template <typename T>
  rocksdb::ColumnFamilyHandle *GetOrErrorCf(const T &shard) const;

  template <typename T>
  std::optional<rocksdb::ColumnFamilyHandle *> GetCf(const T &shard) const;
};

}  // namespace rocks
}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_ROCKSDB_TABLE_PHYSICAL_H__
