#ifndef PELTON_SQL_ROCKSDB_TABLE_H__
#define PELTON_SQL_ROCKSDB_TABLE_H__

#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "pelton/dataflow/schema.h"
#include "pelton/sql/rocksdb/encode.h"
#include "pelton/sql/rocksdb/encryption.h"
#include "pelton/sql/rocksdb/index.h"
#include "rocksdb/db.h"

namespace pelton {
namespace sql {

class LazyVector {
 public:
  explicit LazyVector(rocksdb::Iterator *it) : it_(it) {}

  // Iterator class.
  class Iterator {
   public:
    // Iterator traits
    using value_type = std::pair<EncryptedKey, EncryptedValue>;
    using iterator_category = std::output_iterator_tag;

    Iterator &operator++();

    bool operator==(const Iterator &o) const;
    bool operator!=(const Iterator &o) const;

    // Access current element.
    value_type operator*() const;

   private:
    rocksdb::Iterator *it_;

    Iterator();
    explicit Iterator(rocksdb::Iterator *it);

    void EnsureValid();

    friend LazyVector;
  };

  LazyVector::Iterator begin() const { return Iterator(this->it_.get()); }
  LazyVector::Iterator end() const { return Iterator(); }

 private:
  std::unique_ptr<rocksdb::Iterator> it_;
};

class RocksdbTable {
 public:
  RocksdbTable(rocksdb::DB *db, const std::string &table_name,
               const dataflow::SchemaRef &schema);

  // Index creation.
  void CreateIndex(size_t column_index);
  void CreateIndex(const std::string &column_name) {
    this->CreateIndex(this->schema_.IndexOf(column_name));
  }

  // Index updating.
  void IndexAdd(const rocksdb::Slice &shard, const RocksdbSequence &row);
  void IndexDelete(const rocksdb::Slice &shard, const RocksdbSequence &row);

  // Check if a record with given PK exists.
  bool Exists(const rocksdb::Slice &pk_value) const;

  // Put/Delete API.
  void Put(const EncryptedKey &key, const EncryptedValue &value);
  void Delete(const EncryptedKey &key);

  // Get and MultiGet.
  std::optional<EncryptedValue> Get(const EncryptedKey &key) const;
  std::vector<EncryptedValue> MultiGet(
      const std::unordered_set<EncryptedKey> &keys) const;

  // Read all the data.
  LazyVector GetAll() const;

 private:
  rocksdb::DB *db_;
  std::string table_name_;
  dataflow::SchemaRef schema_;

  // The index of the PK column.
  size_t pk_column_;

  // Column Family handler.
  std::unique_ptr<rocksdb::ColumnFamilyHandle> handle_;

  // Indices.
  RocksdbPKIndex pk_index_;
  std::vector<std::optional<RocksdbIndex>> indices_;  // size == schema_.size().
};

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_ROCKSDB_TABLE_H__
