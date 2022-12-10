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
#include "pelton/sql/rocksdb/table_physical.h"
#include "pelton/sql/rocksdb/transaction.h"
#include "pelton/sqlast/value_mapper.h"
#include "rocksdb/db.h"

namespace pelton {
namespace sql {
namespace rocks {

class RocksdbTable {
 public:
  RocksdbTable(rocksdb::DB *db, const std::string &table_name,
               const dataflow::SchemaRef &schema);

  void AddUniqueColumn(size_t column) {
    this->unique_columns_.push_back(column);
  }

  // Get the schema.
  const dataflow::SchemaRef &Schema() const { return this->schema_; }
  size_t PKColumn() const { return this->pk_column_; }

  // Index creation.
  void CreateIndex(const std::vector<size_t> &columns);
  void CreateIndex(const std::vector<std::string> &column_names) {
    std::vector<size_t> columns;
    for (const std::string &column_name : column_names) {
      columns.push_back(this->schema_.IndexOf(column_name));
    }
    this->CreateIndex(columns);
  }

  // Index updating.
  void IndexAdd(const rocksdb::Slice &shard, const RocksdbSequence &row,
                RocksdbTransaction *txn, bool update_pk = true);
  void IndexDelete(const rocksdb::Slice &shard, const RocksdbSequence &row,
                   RocksdbTransaction *txn, bool update_pk = true);
  void IndexUpdate(const rocksdb::Slice &shard, const RocksdbSequence &old,
                   const RocksdbSequence &updated, RocksdbTransaction *txn);

  // Query planning (selecting index).
  enum IndexChoiceType { PK, UNIQUE, REGULAR, SCAN };
  struct IndexChoice {
    IndexChoiceType type;
    size_t idx;
  };
  IndexChoice ChooseIndex(sqlast::ValueMapper *vm) const;
  IndexChoice ChooseIndex(size_t column_index) const;

  // Index lookups.
  std::optional<IndexSet> IndexLookup(sqlast::ValueMapper *vm,
                                      const RocksdbTransaction *txn) const;
  std::optional<DedupIndexSet> IndexLookupDedup(
      sqlast::ValueMapper *vm, const RocksdbTransaction *txn) const;

  // Get an index.
#ifndef PELTON_PHYSICAL_SEPARATION
  const RocksdbPKIndex &GetPKIndex() const { return this->pk_index_; }
  const RocksdbIndex &GetTableIndex(size_t index) const {
    return this->indices_.at(index);
  }
#endif  // PELTON_PHYSICAL_SEPARATION

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

#ifndef PELTON_PHYSICAL_SEPARATION
  // Check if a record with given PK exists.
  bool Exists(const rocksdb::Slice &pk_value,
              const RocksdbTransaction *txn) const;
#endif  // PELTON_PHYSICAL_SEPARATION

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
  rocksdb::DB *db_;
  std::string table_name_;
  dataflow::SchemaRef schema_;

  // The index of the PK column.
  size_t pk_column_;
  std::vector<size_t> unique_columns_;

  // Wrapper around Column Family handlers.
  RocksdbPhysicalSeparator handle_;

  // Indices.
#ifndef PELTON_PHYSICAL_SEPARATION
  RocksdbPKIndex pk_index_;
#endif  // PELTON_PHYSICAL_SEPARATION
  std::vector<RocksdbIndex> indices_;
};

}  // namespace rocks
}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_ROCKSDB_TABLE_H__
