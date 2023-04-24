#ifndef K9DB_SQL_ROCKSDB_INDEX_H__
#define K9DB_SQL_ROCKSDB_INDEX_H__

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "k9db/dataflow/schema.h"
#include "k9db/sql/rocksdb/dedup.h"
#include "k9db/sql/rocksdb/encode.h"
#include "k9db/sql/rocksdb/metadata.h"
#include "k9db/sql/rocksdb/transaction.h"
#include "k9db/sqlast/value_mapper.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"

namespace k9db {
namespace sql {
namespace rocks {

class RocksdbIndex {
 public:
  // Rocksdb Options for index column families.
  static rocksdb::ColumnFamilyOptions ColumnFamilyOptions();
  static std::string ColumnFamilySuffix();

  // Constructor.
  RocksdbIndex(rocksdb::DB *db, const std::string &table_name,
               const std::vector<size_t> &columns,
               RocksdbMetadata *metadata = nullptr);

  // Get the name used for the rocksdb column family corresponding to this
  // index.
  const std::string &name() const { return this->cf_name_; }

  // Consider table (id PK, name index, age); sharded by some <shard_name>
  // shard_name is the shard_name specialized with the user_id.
  // index_value is the value of 'name'.
  // key is the value of 'id' (the PK) without a shard prefix.
  void Add(const std::vector<rocksdb::Slice> &index_values,
           const rocksdb::Slice &shard_name, const rocksdb::Slice &pk,
           RocksdbWriteTransaction *txn);
  void Delete(const std::vector<rocksdb::Slice> &index_values,
              const rocksdb::Slice &shard_name, const rocksdb::Slice &pk,
              RocksdbWriteTransaction *txn);

  // Use to encode condition values for composite index.
  std::vector<std::string> EncodeComposite(
      sqlast::ValueMapper *vm, const dataflow::SchemaRef &schema) const;

  // Get the shard and pk of matching records for given values.
  IndexSet Get(std::vector<std::string> &&values, const RocksdbInterface *txn,
               int limit = -1) const;
  DedupIndexSet GetDedup(std::vector<std::string> &&values,
                         const RocksdbInterface *txn, int limit = -1) const;

  IndexSet GetWithShards(std::vector<std::string> &&shards,
                         std::vector<std::string> &&values,
                         const RocksdbInterface *txn) const;

  // Get the columns over which this index is defined.
  const std::vector<size_t> &GetColumns() const { return this->columns_; }

 private:
  rocksdb::DB *db_;
  std::unique_ptr<rocksdb::ColumnFamilyHandle> handle_;
  std::vector<size_t> columns_;
  std::string cf_name_;

  using IRecord = RocksdbIndexInternalRecord;
};

class RocksdbPKIndex {
 public:
  // Rocksdb Options for PK index column families.
  static rocksdb::ColumnFamilyOptions ColumnFamilyOptions();
  static std::string ColumnFamilySuffix();

  // Constructor.
  RocksdbPKIndex(rocksdb::DB *db, const std::string &table_name,
                 RocksdbMetadata *metadata = nullptr);

  // Updating.
  void Add(const rocksdb::Slice &pk, const rocksdb::Slice &shard_name,
           RocksdbWriteTransaction *txn);
  void Delete(const rocksdb::Slice &pk, const rocksdb::Slice &shard_name,
              RocksdbWriteTransaction *txn);

  // Check whether PK exists or not and lock it (atomic)!
  bool Exists(const rocksdb::Slice &slice,
              const RocksdbWriteTransaction *txn) const;

  // Reads.
  IndexSet Get(std::vector<std::string> &&pk_values,
               const RocksdbInterface *txn) const;
  DedupIndexSet GetDedup(std::vector<std::string> &&pk_values,
                         const RocksdbInterface *txn) const;

  // Count how many shard each pk value is in.
  std::vector<size_t> CountShards(std::vector<std::string> &&pk_values,
                                  const RocksdbInterface *txn) const;

 private:
  rocksdb::DB *db_;
  std::unique_ptr<rocksdb::ColumnFamilyHandle> handle_;

  using IRecord = RocksdbPKIndexInternalRecord;
};

}  // namespace rocks
}  // namespace sql
}  // namespace k9db

#endif  // K9DB_SQL_ROCKSDB_INDEX_H__
