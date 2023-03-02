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
#include "pelton/sql/rocksdb/metadata.h"
#include "pelton/sql/rocksdb/plan.h"
#include "pelton/sql/rocksdb/transaction.h"
#include "pelton/sqlast/value_mapper.h"
#include "rocksdb/db.h"

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
    rocksdb::Iterator *it_;

    Iterator();
    explicit Iterator(rocksdb::Iterator *it);

    void EnsureValid();

    friend RocksdbStream;
  };

  explicit RocksdbStream(std::unique_ptr<rocksdb::Iterator> &&it)
      : it_(std::move(it)) {}

  RocksdbStream::Iterator begin() const { return Iterator(this->it_.get()); }
  RocksdbStream::Iterator end() const { return Iterator(); }

  // To an actual vector. Consumes this vector.
  std::vector<std::pair<EncryptedKey, EncryptedValue>> ToVector();

 private:
  std::unique_ptr<rocksdb::Iterator> it_;
};

class RocksdbTable {
 public:
  // Rocksdb Options for table column families.
  static rocksdb::ColumnFamilyOptions ColumnFamilyOptions();

  RocksdbTable(rocksdb::DB *db, const std::string &table_name,
               const dataflow::SchemaRef &schema,
               RocksdbMetadata *metadata = nullptr);

  void AddUniqueColumn(size_t column) {
    this->unique_columns_.push_back(column);
  }

  // Get the schema.
  const dataflow::SchemaRef &Schema() const { return this->schema_; }
  size_t PKColumn() const { return this->pk_column_; }

  // Index creation.
  std::string CreateIndex(const std::vector<size_t> &columns);
  std::string CreateIndex(const std::vector<std::string> &column_names) {
    std::vector<size_t> columns;
    for (const std::string &column_name : column_names) {
      columns.push_back(this->schema_.IndexOf(column_name));
    }
    return this->CreateIndex(columns);
  }

  // Index updating.
  void IndexAdd(const rocksdb::Slice &shard, const RocksdbSequence &row,
                RocksdbWriteTransaction *txn, bool update_pk = true);
  void IndexDelete(const rocksdb::Slice &shard, const RocksdbSequence &row,
                   RocksdbWriteTransaction *txn, bool update_pk = true);
  void IndexUpdate(const rocksdb::Slice &shard, const RocksdbSequence &old,
                   const RocksdbSequence &updated,
                   RocksdbWriteTransaction *txn);

  // Query planning (selecting index).
  RocksdbPlan ChooseIndex(const sqlast::ValueMapper *vm) const;
  RocksdbPlan ChooseIndex(size_t column_index) const;

  // Index lookups.
  std::optional<IndexSet> IndexLookup(sqlast::ValueMapper *vm,
                                      const RocksdbInterface *txn,
                                      int limit = -1) const;
  std::optional<DedupIndexSet> IndexLookupDedup(sqlast::ValueMapper *vm,
                                                const RocksdbInterface *txn,
                                                int limit = -1) const;

  // Get an index.
  const RocksdbPKIndex &GetPKIndex() const { return this->pk_index_; }
  const RocksdbIndex &GetTableIndex(size_t index) const {
    return this->indices_.at(index);
  }

  // Index information.
  std::vector<std::string> GetIndices() const {
    std::vector<std::string> result;
    result.push_back("(" + this->schema_.NameOf(this->pk_column_) + ")");
    for (const RocksdbIndex &index : this->indices_) {
      std::string name = "(";
      for (size_t col : index.GetColumns()) {
        name.append(this->schema_.NameOf(col));
        name.push_back(',');
        name.push_back(' ');
      }
      name.pop_back();
      name.back() = ')';
      result.push_back(std::move(name));
    }
    return result;
  }

  // Check if a record with given PK exists.
  bool Exists(const rocksdb::Slice &pk_value,
              const RocksdbWriteTransaction *txn) const;

  // Put/Delete API.
  void Put(const EncryptedKey &key, const EncryptedValue &value,
           RocksdbWriteTransaction *txn);
  void Delete(const EncryptedKey &key, RocksdbWriteTransaction *txn);

  // Get and MultiGet.
  std::optional<EncryptedValue> Get(const EncryptedKey &key,
                                    const RocksdbInterface *txn) const;

  std::vector<std::optional<EncryptedValue>> MultiGet(
      const std::vector<EncryptedKey> &keys, const RocksdbInterface *txn) const;

  // Read all the data.
  RocksdbStream GetAll(const RocksdbInterface *txn) const;

  // Read all data from shard.
  RocksdbStream GetShard(const EncryptedPrefix &shard_name,
                         const RocksdbInterface *txn) const;

 private:
  rocksdb::DB *db_;
  std::string table_name_;
  dataflow::SchemaRef schema_;
  RocksdbMetadata *metadata_;
  // The index of the PK column.
  size_t pk_column_;
  std::vector<size_t> unique_columns_;

  // Column Family handler.
  std::unique_ptr<rocksdb::ColumnFamilyHandle> handle_;

  // Indices.
  RocksdbPKIndex pk_index_;
  std::vector<RocksdbIndex> indices_;
};

}  // namespace rocks
}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_ROCKSDB_TABLE_H__
