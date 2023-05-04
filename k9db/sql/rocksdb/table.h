#ifndef K9DB_SQL_ROCKSDB_TABLE_H__
#define K9DB_SQL_ROCKSDB_TABLE_H__

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "k9db/dataflow/schema.h"
#include "k9db/sql/rocksdb/encode.h"
#include "k9db/sql/rocksdb/encryption.h"
#include "k9db/sql/rocksdb/index.h"
#include "k9db/sql/rocksdb/metadata.h"
#include "k9db/sql/rocksdb/plan.h"
#include "k9db/sql/rocksdb/transaction.h"
#include "k9db/sqlast/value_mapper.h"
#include "rocksdb/db.h"

namespace k9db {
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

    Iterator &operator++() {
      if (this->its_ != nullptr) {
        this->its_->at(this->idx_)->Next();
        this->EnsureValid();
      }
      return *this;
    }

    bool operator==(const Iterator &o) const {
      if (this->its_ == nullptr && o.its_ == nullptr) {
        return true;
      }
      return false;
    }
    bool operator!=(const Iterator &o) const { return !(*this == o); }

    // Access current element.
    value_type operator*() const {
      const auto &it = this->its_->at(this->idx_);
      return value_type(it->key(), Cipher::FromDB(it->value().ToString()));
    }

   private:
    const std::vector<std::unique_ptr<rocksdb::Iterator>> *its_;
    size_t idx_;

    // Constructors.
    Iterator() : its_(nullptr), idx_(0) {}
    explicit Iterator(
        const std::vector<std::unique_ptr<rocksdb::Iterator>> *its)
        : its_(its), idx_(0) {
      this->EnsureValid();
    }

    // Normalize vectors.
    void EnsureValid() {
      if (this->its_ == nullptr) {
        return;
      }
      if (this->its_->size() == 0) {
        this->its_ = nullptr;
        return;
      }
      while (!this->its_->at(this->idx_)->Valid()) {
        this->idx_++;
        if (this->idx_ >= this->its_->size()) {
          this->its_ = nullptr;
          return;
        }
      }
    }

    friend RocksdbStream;
  };

  // Constructors
  RocksdbStream() : its_() {}
  explicit RocksdbStream(std::unique_ptr<rocksdb::Iterator> &&it) : its_() {
    this->AddIterator(std::move(it));
  }

  RocksdbStream::Iterator begin() const { return Iterator(&this->its_); }
  RocksdbStream::Iterator end() const { return Iterator(); }

  // To an actual vector. Consumes this vector.
  std::vector<std::pair<EncryptedKey, EncryptedValue>> ToVector() {
    std::vector<std::pair<EncryptedKey, EncryptedValue>> result;
    for (auto it = this->begin(); it != this->end(); ++it) {
      const auto &[key, cipher] = *it;
      result.emplace_back(key, cipher);
    }
    return result;
  }

  // Add a new Iterator to list (must be done before returning RocksdbStream to
  // consumers).
  void AddIterator(std::unique_ptr<rocksdb::Iterator> &&it) {
    this->its_.push_back(std::move(it));
  }

 private:
  std::vector<std::unique_ptr<rocksdb::Iterator>> its_;
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

#ifndef K9DB_PHYSICAL_SEPARATION
  // Index creation.
  std::string CreateIndex(const std::vector<size_t> &columns);
  std::string CreateIndex(const std::vector<std::string> &column_names) {
    std::vector<size_t> columns;
    for (const std::string &column_name : column_names) {
      columns.push_back(this->schema_.IndexOf(column_name));
    }
    return this->CreateIndex(columns);
  }
#endif  // K9DB_PHYSICAL_SEPARATION

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

#ifndef K9DB_PHYSICAL_SEPARATION
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
#endif  // K9DB_PHYSICAL_SEPARATION

#ifndef K9DB_PHYSICAL_SEPARATION
  // Check if a record with given PK exists.
  bool Exists(const rocksdb::Slice &pk_value,
              const RocksdbWriteTransaction *txn) const;
#endif  // K9DB_PHYSICAL_SEPARATION

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

#ifdef K9DB_PHYSICAL_SEPARATION
  mutable util::UpgradableMutex mtx_;

  // Extracts shard from keys.
  K9dbPrefixTransform transform_;

  using Handle = rocksdb::ColumnFamilyHandle;
  using UniqueHandle = std::unique_ptr<Handle>;
  std::unordered_map<std::string, UniqueHandle> handles_;

  // Get or create a new column family by shard.
  template <typename T>
  rocksdb::ColumnFamilyHandle *GetOrCreateCf(const T &shard);

  template <typename T>
  std::optional<rocksdb::ColumnFamilyHandle *> GetCf(const T &shard) const;
#else
  // Column Family handler.
  std::unique_ptr<rocksdb::ColumnFamilyHandle> handle_;

  // Indices.
  RocksdbPKIndex pk_index_;
  std::vector<RocksdbIndex> indices_;
#endif  // K9DB_PHYSICAL_SEPARATION
};

}  // namespace rocks
}  // namespace sql
}  // namespace k9db

#endif  // K9DB_SQL_ROCKSDB_TABLE_H__
